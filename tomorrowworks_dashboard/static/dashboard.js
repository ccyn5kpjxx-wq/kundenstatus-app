(() => {
    const menuButton = document.querySelector("[data-menu-toggle]");
    const navigation = document.getElementById("hauptnavigation");
    if (menuButton && navigation) {
        menuButton.addEventListener("click", () => {
            const open = navigation.classList.toggle("is-open");
            menuButton.setAttribute("aria-expanded", String(open));
            menuButton.textContent = open ? "Schließen" : "Menü";
        });
    }

    document.querySelectorAll("[data-progress-input]").forEach((input) => {
        const form = input.closest("form");
        const output = form?.querySelector("[data-progress-output]");
        const update = () => { if (output) output.textContent = `${input.value} %`; };
        input.addEventListener("input", update);
        update();
    });

    document.querySelectorAll("[data-file-input]").forEach((input) => {
        const label = input.closest("label")?.querySelector("[data-file-label]");
        input.addEventListener("change", () => {
            if (label) label.textContent = input.files?.[0]?.name || "PDF oder Bild auswählen";
        });
    });

    document.querySelectorAll("[data-sales-video-form]").forEach((form) => {
        form.addEventListener("submit", () => {
            const button = form.querySelector("[data-sales-video-submit]");
            if (!button || button.disabled) return;
            button.disabled = true;
            button.textContent = "Film wird erzeugt …";
            form.classList.add("is-generating-video");
        });
    });

    document.querySelectorAll("[data-copy]").forEach((button) => {
        button.addEventListener("click", async () => {
            try {
                await navigator.clipboard.writeText(button.dataset.copy || "");
                const original = button.textContent;
                button.textContent = "Kopiert ✓";
                window.setTimeout(() => { button.textContent = original; }, 1600);
            } catch {
                button.textContent = "Bitte Pfad markieren";
            }
        });
    });

    document.querySelectorAll("[data-password-toggle]").forEach((button) => {
        const field = button.closest(".password-field")?.querySelector("[data-password-input]");
        if (!field) return;
        button.addEventListener("click", () => {
            const visible = field.type === "text";
            field.type = visible ? "password" : "text";
            button.textContent = visible ? "Passwort anzeigen" : "Passwort verbergen";
            field.focus();
        });
    });

    const offerPresets = {
        website: { paket_name: "Individuelle Unternehmenswebsite", einmalig: "2500,00", monatlich: "0,00", einrichtung: "0,00" },
        ki: { paket_name: "Unternehmenswebsite mit KI-Funktionen", einmalig: "3000,00", monatlich: "300,00", einrichtung: "0,00" },
        betreuung: { paket_name: "Website & laufende digitale Betreuung", einmalig: "2500,00", monatlich: "300,00", einrichtung: "0,00" },
    };
    document.querySelectorAll("[data-offer-preset]").forEach((button) => {
        button.addEventListener("click", () => {
            const preset = offerPresets[button.dataset.offerPreset];
            const form = button.closest("form");
            if (!preset || !form) return;
            Object.entries(preset).forEach(([name, value]) => {
                const field = form.elements.namedItem(name);
                if (field && !field.disabled) field.value = value;
            });
        });
    });

    const gitStates = document.querySelectorAll("[data-project-sync]");
    const updateGitStates = async () => {
        if (!gitStates.length || document.hidden) return;
        try {
            const response = await fetch("/api/projekte/status", {
                credentials: "same-origin",
                headers: { Accept: "application/json" },
            });
            if (!response.ok) return;
            const payload = await response.json();
            (payload.projects || []).forEach((project) => {
                document.querySelectorAll(`[data-project-sync="${project.id}"]`).forEach((container) => {
                    const badge = container.querySelector("[data-git-badge]");
                    const detail = container.querySelector("[data-git-detail]");
                    if (badge) {
                        badge.className = `sync-badge sync-${project.git_status}`;
                        badge.textContent = project.git_label;
                    }
                    if (detail) {
                        const commit = [project.git_kurz, project.git_author].filter(Boolean).join(" · ");
                        detail.textContent = commit || project.git_fehler || "Projektordner verbinden";
                    }
                });
            });
        } catch {
            // Das Dashboard bleibt auch bei einem kurzen Netzwerkaussetzer bedienbar.
        }
    };
    if (gitStates.length) {
        window.setInterval(updateGitStates, 30000);
        document.addEventListener("visibilitychange", updateGitStates);
    }
})();
