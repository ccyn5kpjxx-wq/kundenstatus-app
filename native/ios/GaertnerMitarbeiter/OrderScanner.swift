import AVFoundation
import SwiftUI
import UIKit

struct OrderScanner: View {
    let onScan: (String) -> Void
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            CameraScanner(onScan: onScan)
                .ignoresSafeArea(edges: .bottom)
                .navigationTitle("Auftragscode scannen")
                .navigationBarTitleDisplayMode(.inline)
                .toolbar {
                    ToolbarItem(placement: .cancellationAction) {
                        Button("Abbrechen") { dismiss() }
                    }
                }
        }
    }
}

private struct CameraScanner: UIViewControllerRepresentable {
    let onScan: (String) -> Void

    func makeUIViewController(context: Context) -> ScannerController {
        ScannerController(onScan: onScan)
    }
    func updateUIViewController(_ controller: ScannerController, context: Context) {}
    static func dismantleUIViewController(_ controller: ScannerController, coordinator: ()) {
        controller.stop()
    }
}

private final class ScannerController: UIViewController, AVCaptureMetadataOutputObjectsDelegate {
    private let capture = AVCaptureSession()
    private let queue = DispatchQueue(label: "de.gaertner.order-scanner")
    private let onScan: (String) -> Void
    private let message = UILabel()
    private var preview: AVCaptureVideoPreviewLayer?
    private var active = false
    private var configured = false
    private var delivered = false

    init(onScan: @escaping (String) -> Void) {
        self.onScan = onScan
        super.init(nibName: nil, bundle: nil)
    }
    required init?(coder: NSCoder) { nil }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        message.text = "QR-Code der Auftragsnummer in die Kamera halten."
        message.textColor = .white
        message.numberOfLines = 0
        message.textAlignment = .center
        message.translatesAutoresizingMaskIntoConstraints = false
        message.backgroundColor = UIColor.black.withAlphaComponent(0.7)
        view.addSubview(message)
        NSLayoutConstraint.activate([
            message.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 20),
            message.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -20),
            message.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -24)
        ])
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        active = true
        switch AVCaptureDevice.authorizationStatus(for: .video) {
        case .authorized: start()
        case .notDetermined:
            AVCaptureDevice.requestAccess(for: .video) { [weak self] granted in
                DispatchQueue.main.async {
                    guard let self, self.active else { return }
                    if granted { self.start() } else { self.permissionDenied() }
                }
            }
        default: permissionDenied()
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        stop()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        preview?.frame = view.bounds
    }

    func stop() {
        active = false
        // All start/stop calls are serialized. Closing during a pending permission
        // request cannot start the camera later in the background.
        queue.async { [capture] in
            if capture.isRunning { capture.stopRunning() }
        }
    }

    private func permissionDenied() {
        message.text = "Kamera nicht freigegeben. Du kannst den Scanner schließen und die Auftragsnummer eingeben."
    }

    private func start() {
        guard active, !delivered else { return }
        if !configured {
            guard let device = AVCaptureDevice.default(for: .video),
                  let input = try? AVCaptureDeviceInput(device: device),
                  capture.canAddInput(input) else {
                message.text = "Keine Kamera verfügbar. Bitte die Auftragsnummer eingeben."
                return
            }
            capture.beginConfiguration()
            capture.addInput(input)
            let output = AVCaptureMetadataOutput()
            guard capture.canAddOutput(output) else {
                capture.commitConfiguration()
                message.text = "Der Scanner ist auf diesem Gerät nicht verfügbar."
                return
            }
            capture.addOutput(output)
            output.setMetadataObjectsDelegate(self, queue: .main)
            output.metadataObjectTypes = [.qr]
            capture.commitConfiguration()
            let preview = AVCaptureVideoPreviewLayer(session: capture)
            preview.videoGravity = .resizeAspectFill
            preview.frame = view.bounds
            view.layer.insertSublayer(preview, at: 0)
            self.preview = preview
            configured = true
        }
        queue.async { [capture] in
            if !capture.isRunning { capture.startRunning() }
        }
    }

    func metadataOutput(_ output: AVCaptureMetadataOutput, didOutput metadataObjects: [AVMetadataObject], from connection: AVCaptureConnection) {
        guard active, !delivered,
              let code = metadataObjects.compactMap({ $0 as? AVMetadataMachineReadableCodeObject }).first,
              let payload = code.stringValue else { return }
        delivered = true
        stop()
        onScan(payload)
    }
}
