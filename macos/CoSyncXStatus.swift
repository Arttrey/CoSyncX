import AppKit
import Foundation

// The helper reports transfer status only; it cannot certify chat restoration.
class StatusApp: NSObject, NSApplicationDelegate {
    private var item: NSStatusItem!
    private var timer: Timer?
    private var checking = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "CoSyncX: проверка"
        let menu = NSMenu()
        let quit = NSMenuItem(title: "Закрыть CoSyncX Status", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
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
                let labels = ["TRANSFERRING": "CoSyncX: передача", "TRANSFERRED": "CoSyncX: ожидают восстановления",
                              "RESTORING": "CoSyncX: восстановление", "RESTORED": "CoSyncX: восстановлено",
                              "PENDING": "CoSyncX: ожидание",
                              "DISCONNECTED": "CoSyncX: VPS недоступен", "ERROR": "CoSyncX: ошибка Syncthing",
                              "UNAVAILABLE": "CoSyncX: нет связи с Syncthing"]
                self.item.button?.title = labels[state] ?? labels["UNAVAILABLE"]!
                self.item.button?.toolTip = state == "RESTORED"
                    ? "CoSyncX подтвердил восстановление и индексацию чатов."
                    : "Статус передачи чатов; восстановление и индексация не подтверждены."
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
