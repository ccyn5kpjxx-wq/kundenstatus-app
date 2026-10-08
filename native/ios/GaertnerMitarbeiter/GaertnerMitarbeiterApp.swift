import SwiftUI
import GaertnerCore

@main
struct GaertnerMitarbeiterApp: App {
    var body: some Scene {
        WindowGroup { MitarbeiterHome() }
    }
}

@MainActor
struct MitarbeiterHome: View {
    @StateObject private var session = PortalSession(policy: PortalPolicy())
    @Environment(\.scenePhase) private var scenePhase
    @State private var showOrderFinder = false
    private let green = Color(red: 23 / 255, green: 63 / 255, blue: 53 / 255)

    var body: some View {
        ZStack {
            NavigationStack {
                VStack(spacing: 0) {
                    if session.isLoading { ProgressView().tint(green).padding(5) }
                    if let error = session.errorMessage {
                        VStack(alignment: .leading, spacing: 8) {
                            Text(error).font(.callout).accessibilityAddTraits(.isStaticText)
                            Button("Erneut laden") { session.reload() }.font(.callout.bold())
                        }
                        .padding().frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color(red: 1, green: 0.94, blue: 0.85))
                    }
                    PortalWebView(session: session)
                }
                .navigationTitle("Gärtner")
                .navigationBarTitleDisplayMode(.inline)
                .toolbar {
                    ToolbarItem(placement: .topBarLeading) {
                        Menu {
                            ForEach(PortalDestination.allCases) { destination in
                                Button {
                                    session.open(session.policy.url(for: destination))
                                } label: {
                                    Label(destination.title, systemImage: destination.symbol)
                                }
                            }
                        } label: {
                            Label("Menü", systemImage: "line.3.horizontal")
                        }
                        .accessibilityLabel("Mitarbeitermenü öffnen")
                    }
                    ToolbarItem(placement: .topBarTrailing) {
                        Button { showOrderFinder = true } label: {
                            Label("Auftrag öffnen", systemImage: "qrcode.viewfinder")
                        }
                        .accessibilityLabel("Auftragsnummer eingeben oder scannen")
                    }
                }
                .sheet(isPresented: $showOrderFinder) {
                    OrderFinder(policy: session.policy) { url in
                        showOrderFinder = false
                        session.open(url)
                    }
                }
            }
            // Cover the portal before iOS takes its app-switcher snapshot.
            // This is a privacy cover, not an alternate authentication mechanism.
            if scenePhase != .active {
                Color(red: 241 / 255, green: 245 / 255, blue: 236 / 255)
                    .ignoresSafeArea()
                    .overlay {
                        VStack(spacing: 14) {
                            Image(systemName: "lock.shield").font(.system(size: 42))
                            Text("Gärtner").font(.title.bold())
                            Text("Dein Mitarbeiterkonto").font(.callout)
                        }.foregroundStyle(green)
                    }
                    .accessibilityLabel("Persönliche Ansicht geschützt")
            }
        }
        .tint(green)
        .onChange(of: scenePhase) { _, phase in
            if phase == .active { session.resumeMediaPlayback() }
            else if phase == .background {
                showOrderFinder = false
                session.suspendMedia()
            }
        }
    }
}

private struct OrderFinder: View {
    let policy: PortalPolicy
    let openOrder: (URL) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var number = ""
    @State private var scanning = false
    @State private var scanMessage: String?

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    Text("Die Auftragsnummer hängt am Fahrzeug.")
                    TextField("Zum Beispiel 102", text: $number)
                        .keyboardType(.numberPad).textContentType(.none)
                        .accessibilityLabel("Auftragsnummer")
                    if let url = policy.orderURL(for: number) {
                        Button("Auftrag \(policy.orderNumber(from: number) ?? number) öffnen") {
                            openOrder(url)
                        }
                    } else {
                        Text("Auftragsnummer eingeben oder den QR-Code am Fahrzeug scannen.")
                            .foregroundStyle(.secondary).font(.footnote)
                    }
                }
                Section {
                    Button { scanning = true } label: {
                        Label("Auftragsnummer scannen", systemImage: "qrcode.viewfinder")
                    }
                    if let scanMessage { Text(scanMessage).font(.callout) }
                } footer: {
                    Text("Der Scanner öffnet keine fremden Links. Die erkannte Auftragsnummer bestätigst du oben.")
                }
            }
            .navigationTitle("Auftrag öffnen")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Schließen") { dismiss() }
                }
            }
            .sheet(isPresented: $scanning) {
                OrderScanner { payload in
                    scanning = false
                    if let found = policy.orderNumber(from: payload) {
                        number = found
                        scanMessage = "Auftrag \(found) erkannt. Zum Öffnen oben bestätigen."
                    } else {
                        scanMessage = "Das ist kein Auftragscode. Bitte die Nummer vom Fahrzeug eingeben."
                    }
                }
            }
        }
    }
}
