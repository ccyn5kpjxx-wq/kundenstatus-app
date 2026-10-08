import Foundation

public enum NavigationDecision: Equatable {
    case internalPortal, externalWebsite, externalContact, blocked
}

/// URLs are navigation inputs, never authority to read or change a record.
/// The existing server must still authenticate and authorize every request.
public struct PortalPolicy {
    public let baseURL = URL(string: "https://kundenstatus-app.onrender.com")!

    public init() {}

    public var startURL: URL { url(for: .profile) }

    public func hasTrustedOrigin(_ url: URL) -> Bool {
        guard let parts = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return false }
        return parts.scheme?.lowercased() == "https" &&
            parts.host?.lowercased() == baseURL.host &&
            (parts.port == nil || parts.port == 443) &&
            parts.user == nil && parts.password == nil
    }

    public func navigationDecision(for url: URL) -> NavigationDecision {
        guard let parts = URLComponents(url: url, resolvingAgainstBaseURL: false),
              parts.user == nil, parts.password == nil,
              let decoded = url.absoluteString.removingPercentEncoding,
              !decoded.unicodeScalars.contains(where: CharacterSet.controlCharacters.contains) else {
            return .blocked
        }
        if hasTrustedOrigin(url) {
            let path = parts.percentEncodedPath
            let lower = path.lowercased()
            // Reject encoded separators and traversal before WebKit or the server
            // normalizes the URL. No administrator routes in the native shell.
            guard path.hasPrefix("/werkstatt/"), !lower.contains("%2f"),
                  !lower.contains("%5c"), !path.contains("\\"),
                  !path.split(separator: "/").contains(where: {
                      let segment = String($0).removingPercentEncoding
                      return segment == "." || segment == ".."
                  }) else { return .blocked }
            return .internalPortal
        }
        if parts.scheme?.lowercased() == "https", parts.host?.isEmpty == false,
           parts.host?.lowercased() != baseURL.host {
            // The WebKit coordinator additionally requires an explicit link tap
            // from our own portal frame before handing this to the system browser.
            return .externalWebsite
        }
        if ["tel", "mailto"].contains(parts.scheme?.lowercased() ?? ""), !parts.path.isEmpty {
            return .externalContact
        }
        return .blocked
    }

    public func url(for destination: PortalDestination) -> URL {
        URL(string: destination.path, relativeTo: baseURL)!.absoluteURL
    }

    public func orderURL(for number: String) -> URL? {
        guard let number = normalizedNumber(number) else { return nil }
        var parts = URLComponents(url: url(for: .orders), resolvingAgainstBaseURL: false)!
        parts.queryItems = [URLQueryItem(name: "nummer", value: number)]
        return parts.url
    }

    /// Accept an explicit order number or our exact order-search QR URL only.
    /// QR contents never open a website or submit an operation on their own.
    public func orderNumber(from payload: String) -> String? {
        let value = payload.trimmingCharacters(in: .whitespacesAndNewlines)
        guard value.count <= 512 else { return nil }
        if let number = normalizedNumber(value) { return number }
        if value.lowercased().hasPrefix("auftrag ") {
            return normalizedNumber(String(value.dropFirst("auftrag ".count)))
        }
        guard let url = URL(string: value), hasTrustedOrigin(url),
              let parts = URLComponents(url: url, resolvingAgainstBaseURL: false),
              parts.percentEncodedPath == "/werkstatt/mein-konto/auftraege",
              parts.fragment == nil, let items = parts.queryItems,
              items.count == 1, items[0].name == "nummer", let number = items[0].value else {
            return nil
        }
        return normalizedNumber(number)
    }

    private func normalizedNumber(_ value: String) -> String? {
        let digits = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !digits.isEmpty, digits.count <= 10,
              digits.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }),
              let number = UInt64(digits), number > 0 else { return nil }
        return String(number)
    }
}

public enum PortalDestination: String, CaseIterable, Identifiable {
    case profile, orders, material, time, leave, payroll, documents, assistant
    public var id: String { rawValue }
    public var title: String {
        switch self {
        case .profile: return "Mein Profil"
        case .orders: return "Aufträge"
        case .material: return "Nachbestellen"
        case .time: return "Arbeitszeit"
        case .leave: return "Urlaub"
        case .payroll: return "Lohnzettel"
        case .documents: return "Unterlagen"
        case .assistant: return "Sprachassistent"
        }
    }
    public var symbol: String {
        switch self {
        case .profile: return "person.crop.circle"
        case .orders: return "car.side"
        case .material: return "shippingbox"
        case .time: return "clock"
        case .leave: return "sun.max"
        case .payroll: return "doc.text"
        case .documents: return "folder"
        case .assistant: return "waveform"
        }
    }
    public var path: String {
        switch self {
        case .profile: return "/werkstatt/mein-konto"
        case .orders: return "/werkstatt/mein-konto/auftraege"
        case .material: return "/werkstatt/materialbestellung"
        case .time: return "/werkstatt/assistent/arbeitszeit"
        case .leave: return "/werkstatt/assistent/urlaub"
        case .payroll: return "/werkstatt/mein-konto#lohnzettel"
        case .documents: return "/werkstatt/mein-konto#arbeitsvertraege"
        case .assistant: return "/werkstatt/assistent"
        }
    }
}
