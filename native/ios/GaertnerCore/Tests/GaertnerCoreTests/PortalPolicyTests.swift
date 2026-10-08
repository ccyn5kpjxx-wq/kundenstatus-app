import XCTest
@testable import GaertnerCore

final class PortalPolicyTests: XCTestCase {
    let policy = PortalPolicy()

    func testAllDestinationsUsePersonalPortal() {
        for destination in PortalDestination.allCases {
            let url = policy.url(for: destination)
            XCTAssertTrue(policy.hasTrustedOrigin(url))
            XCTAssertEqual(policy.navigationDecision(for: url), .internalPortal)
        }
        XCTAssertEqual(policy.startURL.path, "/werkstatt/mein-konto")
    }

    func testOriginDoesNotTrustSimilarHostsOrCredentials() {
        let invalid = [
            "http://kundenstatus-app.onrender.com/werkstatt/mein-konto",
            "https://kundenstatus-app.onrender.com.evil.example/werkstatt/mein-konto",
            "https://evil.example/werkstatt/mein-konto",
            "https://kundenstatus-app.onrender.com:444/werkstatt/mein-konto",
            "https://user@kundenstatus-app.onrender.com/werkstatt/mein-konto",
            "https://kundenstatus-app.onrender.com@evil.example/werkstatt/mein-konto"
        ]
        for value in invalid { XCTAssertFalse(policy.hasTrustedOrigin(URL(string: value)!), value) }
        XCTAssertTrue(policy.hasTrustedOrigin(URL(string: "https://kundenstatus-app.onrender.com:443")!))
    }

    func testNavigationBlocksAdminScriptsFilesAndTraversal() {
        for value in [
            "https://kundenstatus-app.onrender.com/admin/cockpit",
            "https://kundenstatus-app.onrender.com:444/werkstatt/mein-konto",
            "https://kundenstatus-app.onrender.com/werkstatt/../admin",
            "https://kundenstatus-app.onrender.com/werkstatt/%2e%2e/admin",
            "https://kundenstatus-app.onrender.com/werkstatt/%2fadmin",
            "https://kundenstatus-app.onrender.com/werkstatt/%5cadmin",
            "javascript:alert(1)", "file:///etc/passwd", "data:text/html,hello",
            "http://external.example/", "mailto:someone@example.org?subject=Hi%0aBcc:other@example.org"
        ] {
            XCTAssertEqual(policy.navigationDecision(for: URL(string: value)!), .blocked, value)
        }
        XCTAssertEqual(policy.navigationDecision(for: URL(string: "https://example.org/support")!), .externalWebsite)
        XCTAssertEqual(policy.navigationDecision(for: URL(string: "tel:+491234567")!), .externalContact)
        XCTAssertEqual(policy.navigationDecision(for: URL(string: "mailto:info@example.org")!), .externalContact)
    }

    func testOnlyOrderNumbersAndExactPortalOrderLinksAreRecognized() {
        XCTAssertEqual(policy.orderNumber(from: "102"), "102")
        XCTAssertEqual(policy.orderNumber(from: "  Auftrag 102\n"), "102")
        XCTAssertEqual(policy.orderNumber(from: "000102"), "102")
        XCTAssertEqual(policy.orderNumber(from: "https://kundenstatus-app.onrender.com/werkstatt/mein-konto/auftraege?nummer=102"), "102")
        for payload in [
            "0", "-2", "1.2", "102 bestellen", "１２３", "10000000000",
            "https://evil.example/werkstatt/mein-konto/auftraege?nummer=102",
            "https://kundenstatus-app.onrender.com/admin?nummer=102",
            "https://kundenstatus-app.onrender.com/werkstatt/mein-konto/auftraege?nummer=102&nummer=103",
            "https://kundenstatus-app.onrender.com/werkstatt/mein-konto/auftraege?nummer=102&token=secret",
            "https://kundenstatus-app.onrender.com/werkstatt/mein-konto/auftraege?nummer=102#anything",
            "javascript:102", String(repeating: "1", count: 513)
        ] {
            XCTAssertNil(policy.orderNumber(from: payload), payload)
        }
    }

    func testSearchBuildsOnlyAReadOnlyNumberQuery() {
        let url = policy.orderURL(for: "00102")!
        XCTAssertEqual(url.absoluteString, "https://kundenstatus-app.onrender.com/werkstatt/mein-konto/auftraege?nummer=102")
        XCTAssertNil(policy.orderURL(for: "102&confirmed=ja"))
        XCTAssertNil(policy.orderURL(for: "javascript:102"))
    }
}
