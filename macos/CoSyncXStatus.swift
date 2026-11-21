import AppKit
import Foundation

// The helper reports transfer status only; it cannot certify chat restoration.
class StatusApp: NSObject, NSApplicationDelegate {
    private var item: NSStatusItem!
    private var timer: Timer?
    private var checking = false
    private var detail: NSMenuItem!

    func applicationDidFinishLaunching(_ notification: Notification) {
        item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "CoSyncX: checking"
        let menu = NSMenu()
        detail = NSMenuItem(title: "Checking chat status", action: nil, keyEquivalent: "")
        menu.addItem(detail)
        menu.addItem(NSMenuItem.separator())
        let quit = NSMenuItem(title: "Quit CoSyncX Status", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        menu.addItem(quit)
        item.menu = menu
        refresh()
        timer = Timer.scheduledTimer(withTimeInterval: 15, repeats: true) { [weak self] _ in self?.refresh() }
    }

    private func refresh() {
        guard !checking, CommandLine.arguments.count >= 3 else { return }
        checking = true
        let arguments = Array(CommandLine.arguments.dropFirst())
        DispatchQueue.global(qos: .utility).async {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: arguments[0])
            process.arguments = Array(arguments.dropFirst())
            let output = Pipe()
            process.standardOutput = output
            process.standardError = FileHandle.nullDevice
            var state = "UNAVAILABLE"
            do {
                try process.run()
                let data = output.fileHandleForReading.readDataToEndOfFile()
                process.waitUntilExit()
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let value = json["state"] as? String { state = value }
            } catch { }
            DispatchQueue.main.async {
                let labels: [String: (String, String)] = [
                    "TRANSFERRING": ("CoSyncX: transferring", "Chat files are being transferred through Syncthing."),
                    "TRANSFERRED": ("CoSyncX: files received", "Transfer complete; chat restoration is not confirmed."),
                    "RESTORING": ("CoSyncX: restoring", "CoSyncX is restoring chats and updating their index."),
                    "RESTORED": ("CoSyncX: restored", "Chat restoration and indexing have been confirmed."),
                    "PENDING": ("CoSyncX: awaiting restore", "Restoration is pending, for example while Cursor is open or a chat conflict needs attention."),
                    "DISCONNECTED": ("CoSyncX: VPS offline", "The device sending chats is disconnected."),
                    "ERROR": ("CoSyncX: error", "Check Syncthing and the restoration service."),
                    "UNAVAILABLE": ("CoSyncX: unavailable", "Unable to read the Syncthing status.")
                ]
                let label = labels[state] ?? labels["UNAVAILABLE"]!
                self.item.button?.title = label.0
                self.item.button?.toolTip = label.1
                self.detail.title = label.1
                self.checking = false
            }
        }
    }
}

let app = NSApplication.shared
let delegate = StatusApp()
app.delegate = delegate
app.setActivationPolicy(.accessory)
app.run()
