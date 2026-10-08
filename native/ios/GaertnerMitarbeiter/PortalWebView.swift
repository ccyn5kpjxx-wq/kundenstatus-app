import Combine
import Foundation
import GaertnerCore
import SwiftUI
import UIKit
import WebKit

/// The portal owns authentication and authorization. WebKit alone keeps its cookies.
@MainActor
final class PortalSession: ObservableObject {
    @Published var isLoading = false
    @Published var canGoBack = false
    @Published var errorMessage: String?

    let policy: PortalPolicy
    weak var webView: WKWebView?
    fileprivate var isMediaSuspended = false
    private var pendingURL: URL?

    init(policy: PortalPolicy) {
        self.policy = policy
    }

    func open(_ url: URL) {
        guard case .internalPortal = policy.navigationDecision(for: url) else {
            errorMessage = "Dieser Link gehört nicht zum Mitarbeiterportal."
            return
        }
        guard let webView else {
            pendingURL = url
            return
        }
        if errorMessage != nil {
            errorMessage = nil
        }
        // Retry is always a fresh GET, never a replay of an order or time-entry POST.
        let request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
        webView.load(request)
    }

    func reload() {
        open(webView?.url ?? policy.startURL)
    }

    func goBack() {
        guard let webView, webView.canGoBack else { return }
        errorMessage = nil
        webView.goBack()
    }

    func suspendMedia() {
        isMediaSuspended = true
        webView?.setMicrophoneCaptureState(.none, completionHandler: nil)
        webView?.setCameraCaptureState(.none, completionHandler: nil)
        webView?.setAllMediaPlaybackSuspended(true, completionHandler: nil)
    }

    func resumeMediaPlayback() {
        isMediaSuspended = false
        // Capture stays stopped; this never reactivates the microphone or camera.
        webView?.setAllMediaPlaybackSuspended(false, completionHandler: nil)
    }

    fileprivate func attach(_ webView: WKWebView) {
        self.webView = webView
        if isMediaSuspended {
            suspendMedia()
        }
        let initialURL = pendingURL ?? policy.startURL
        pendingURL = nil
        open(initialURL)
    }

    fileprivate func updateState(from webView: WKWebView) {
        guard self.webView === webView else { return }
        if isLoading != webView.isLoading {
            isLoading = webView.isLoading
        }
        if canGoBack != webView.canGoBack {
            canGoBack = webView.canGoBack
        }
    }
}

@MainActor
struct PortalWebView: UIViewRepresentable {
    let session: PortalSession

    func makeCoordinator() -> Coordinator {
        Coordinator(session: session)
    }

    func makeUIView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        // Cookies exist only during the running app session. Restarting this
        // prototype requires signing in again; no WebKit cache survives a restart.
        configuration.websiteDataStore = .nonPersistent()
        configuration.allowsInlineMediaPlayback = true
        // Portal controls the conversation start; its asynchronous WebRTC reply must
        // not be blocked by a second media-playback gesture requirement.
        configuration.mediaTypesRequiringUserActionForPlayback = []
        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.allowsBackForwardNavigationGestures = false
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        context.coordinator.observe(webView)
        session.attach(webView)
        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {
        // Published loading/error changes must not reload forms or replay requests.
    }

    static func dismantleUIView(_ webView: WKWebView, coordinator: Coordinator) {
        webView.stopLoading()
        webView.navigationDelegate = nil
        webView.uiDelegate = nil
        coordinator.invalidateObservations()
        if coordinator.session.webView === webView {
            coordinator.session.webView = nil
        }
    }

    @MainActor
    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate {
        let session: PortalSession
        private var observations: [NSKeyValueObservation] = []
        private let downloadMessage = "Dokument-Downloads sind in dieser App-Version noch nicht verfügbar. Es wurde keine Datei gespeichert."

        init(session: PortalSession) {
            self.session = session
        }

        func observe(_ webView: WKWebView) {
            observations = [
                webView.observe(\.isLoading, options: [.initial, .new]) { [weak self] webView, _ in
                    Task { @MainActor [weak self, weak webView] in
                        guard let self, let webView else { return }
                        self.session.updateState(from: webView)
                    }
                },
                webView.observe(\.canGoBack, options: [.initial, .new]) { [weak self] webView, _ in
                    Task { @MainActor [weak self, weak webView] in
                        guard let self, let webView else { return }
                        self.session.updateState(from: webView)
                    }
                }
            ]
        }

        func invalidateObservations() {
            observations.forEach { $0.invalidate() }
            observations.removeAll()
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationAction: WKNavigationAction,
            decisionHandler: @escaping @MainActor @Sendable (WKNavigationActionPolicy) -> Void
        ) {
            guard let url = navigationAction.request.url else {
                decisionHandler(.cancel)
                session.errorMessage = "Der Link konnte nicht geöffnet werden."
                return
            }

            switch session.policy.navigationDecision(for: url) {
            case .internalPortal:
                if navigationAction.shouldPerformDownload {
                    decisionHandler(.cancel)
                    session.errorMessage = downloadMessage
                } else if navigationAction.targetFrame == nil {
                    decisionHandler(.cancel)
                    openNewWindowRequest(navigationAction)
                } else {
                    decisionHandler(.allow)
                }
            case .externalWebsite, .externalContact:
                decisionHandler(.cancel)
                openExternalLink(navigationAction, url: url)
            case .blocked:
                decisionHandler(.cancel)
                session.errorMessage = "Dieser Link kann in der Mitarbeiter-App nicht geöffnet werden."
            }
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationResponse: WKNavigationResponse,
            decisionHandler: @escaping @MainActor @Sendable (WKNavigationResponsePolicy) -> Void
        ) {
            guard let url = navigationResponse.response.url,
                  session.policy.hasTrustedOrigin(url) else {
                decisionHandler(.cancel)
                session.errorMessage = "Eine Weiterleitung außerhalb des Mitarbeiterportals wurde gestoppt."
                return
            }
            let response = navigationResponse.response as? HTTPURLResponse
            let disposition = response?.value(forHTTPHeaderField: "Content-Disposition")?
                .trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard navigationResponse.canShowMIMEType,
                  disposition?.hasPrefix("attachment") != true else {
                decisionHandler(.cancel)
                session.errorMessage = downloadMessage
                return
            }
            if navigationResponse.isForMainFrame, let response, response.statusCode >= 400 {
                session.errorMessage = "Das Portal meldet einen Fehler (\(response.statusCode)). Bitte versuche es erneut."
            }
            decisionHandler(.allow)
        }

        func webView(
            _ webView: WKWebView,
            createWebViewWith configuration: WKWebViewConfiguration,
            for navigationAction: WKNavigationAction,
            windowFeatures: WKWindowFeatures
        ) -> WKWebView? {
            // Do not move authenticated documents into a separate Safari cookie jar.
            openNewWindowRequest(navigationAction)
            return nil
        }

        @available(iOS 15.0, *)
        func webView(
            _ webView: WKWebView,
            requestMediaCapturePermissionFor origin: WKSecurityOrigin,
            initiatedByFrame frame: WKFrameInfo,
            type: WKMediaCaptureType,
            decisionHandler: @escaping @MainActor @Sendable (WKPermissionDecision) -> Void
        ) {
            guard !session.isMediaSuspended,
                  frame.isMainFrame,
                  isTrusted(origin),
                  isTrusted(frame.securityOrigin),
                  let pageURL = webView.url,
                  session.policy.hasTrustedOrigin(pageURL) else {
                decisionHandler(.deny)
                return
            }
            decisionHandler(.prompt)
        }

        func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
            session.errorMessage = nil
            session.updateState(from: webView)
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            session.updateState(from: webView)
        }

        func webView(
            _ webView: WKWebView,
            didFailProvisionalNavigation navigation: WKNavigation!,
            withError error: Error
        ) {
            showNavigationError(error, webView: webView)
        }

        func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
            showNavigationError(error, webView: webView)
        }

        func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
            session.isLoading = false
            session.errorMessage = "Die Portalansicht wurde beendet. Bitte lade sie erneut."
            session.canGoBack = webView.canGoBack
        }

        private func openNewWindowRequest(_ action: WKNavigationAction) {
            guard let url = action.request.url,
                  case .internalPortal = session.policy.navigationDecision(for: url),
                  (action.request.httpMethod ?? "GET").uppercased() == "GET",
                  action.request.httpBody == nil,
                  action.request.httpBodyStream == nil else {
                session.errorMessage = "Diese Aktion kann nicht in einem neuen Fenster ausgeführt werden. Bitte öffne sie direkt im Portal."
                return
            }
            session.open(url)
        }

        private func openExternalLink(_ action: WKNavigationAction, url: URL) {
            guard action.navigationType == .linkActivated,
                  isTrusted(action.sourceFrame.securityOrigin),
                  action.targetFrame?.isMainFrame != false,
                  (action.request.httpMethod ?? "GET").uppercased() == "GET" else {
                session.errorMessage = "Eine automatische Weiterleitung außerhalb des Mitarbeiterportals wurde gestoppt."
                return
            }
            UIApplication.shared.open(url, options: [:]) { [weak session] opened in
                Task { @MainActor [weak session] in
                    if !opened {
                        session?.errorMessage = "Für diesen Link ist auf dem iPhone keine passende App verfügbar."
                    }
                }
            }
        }

        private func isTrusted(_ origin: WKSecurityOrigin) -> Bool {
            var components = URLComponents()
            components.scheme = origin.protocol
            components.host = origin.host
            if origin.port != 0 {
                components.port = origin.port
            }
            guard let url = components.url else { return false }
            return session.policy.hasTrustedOrigin(url)
        }

        private func showNavigationError(_ error: Error, webView: WKWebView) {
            let failure = error as NSError
            guard !(failure.domain == NSURLErrorDomain && failure.code == NSURLErrorCancelled) else {
                return
            }
            session.updateState(from: webView)
            session.isLoading = false
            session.errorMessage = "Das Mitarbeiterportal konnte nicht geladen werden. Bitte prüfe deine Internetverbindung und versuche es erneut."
        }
    }
}
