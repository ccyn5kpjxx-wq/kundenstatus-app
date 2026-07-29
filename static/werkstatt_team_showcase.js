(function () {
  const messungenDialog = document.querySelector('[data-messungen-dialog]');
      // Team-Motivation als kurze Diashow. Der letzte Lauf wird lokal gespeichert,
      // damit der automatische Minuten-Reload den 5-Minuten-Rhythmus nicht zuruecksetzt.
      const teamShowcase = document.querySelector('[data-team-showcase]');
      const teamOeffnen = document.querySelector('[data-team-oeffnen]');
      const teamSchliessen = document.querySelector('[data-team-schliessen]');
      const teamTonKnopf = document.querySelector('[data-team-ton]');
      const teamMusik = document.querySelector('[data-team-musik]');
      const teamSlides = teamShowcase ? Array.from(teamShowcase.querySelectorAll('[data-team-slide]')) : [];
      const teamFortschritt = teamShowcase ? Array.from(teamShowcase.querySelectorAll('[data-team-fortschritt] span')) : [];
      const teamIntervall = teamShowcase ? Number(teamShowcase.dataset.teamIntervall) || 300000 : 300000;
      const teamSlideDauer = teamShowcase ? Number(teamShowcase.dataset.teamSlideDauer) || 4000 : 4000;
      const teamErsterStart = teamIntervall;
      const teamSpeicherKey = 'werkstatt-team-showcase-letzter-start-v2';
      const teamTonKey = 'werkstatt-team-showcase-ton-v1';
      let teamTimer = null;
      let teamSlideTimer = null;
      let teamAktuellerSlide = 0;
      let teamVorherigerFokus = null;
      let teamLetzteInteraktion = Date.now();
      let teamTonAktiv = true;

      const teamSpeicherLesen = (key) => {
        try { return window.localStorage.getItem(key); } catch (fehler) { return null; }
      };
      const teamSpeicherSchreiben = (key, wert) => {
        try { window.localStorage.setItem(key, String(wert)); } catch (fehler) {}
      };

      teamTonAktiv = teamSpeicherLesen(teamTonKey) !== 'aus';

      const teamTonAnzeigeAktualisieren = () => {
        if (!teamTonKnopf) return;
        const melodieLaeuft = teamMusik && !teamMusik.paused;
        teamTonKnopf.setAttribute('aria-pressed', teamTonAktiv ? 'true' : 'false');
        teamTonKnopf.textContent = !teamTonAktiv
          ? '🔇 Melodie aus'
          : (melodieLaeuft ? '🔊 Melodie an' : '🔊 Melodie aktivieren');
      };

      const teamMelodieStoppen = () => {
        if (!teamMusik) return;
        try {
          teamMusik.pause();
          teamMusik.currentTime = 0;
        } catch (fehler) {}
        teamTonAnzeigeAktualisieren();
      };

      // Richtige, lokal ausgelieferte Kinomusik statt einer synthetischen Web-Audio-Fanfare.
      const teamMelodieSpielen = async () => {
        if (!teamTonAktiv || !teamMusik) return false;
        try {
          teamMusik.pause();
          teamMusik.currentTime = 0;
          teamMusik.volume = .72;
          await teamMusik.play();
          teamTonAnzeigeAktualisieren();
          return true;
        } catch (fehler) {
          teamTonAnzeigeAktualisieren();
          return false;
        }
      };

      if (teamMusik) {
        ['play', 'pause', 'ended', 'error'].forEach((ereignis) => {
          teamMusik.addEventListener(ereignis, teamTonAnzeigeAktualisieren);
        });
      }

      const teamSlideDauerHolen = (index) => {
        const eigeneDauer = Number(teamSlides[index]?.dataset.teamDauer);
        return eigeneDauer > 0 ? eigeneDauer : teamSlideDauer;
      };

      const teamSlideZeigen = (index) => {
        teamAktuellerSlide = index;
        const aktuelleDauer = teamSlideDauerHolen(index);
        teamSlides.forEach((slide, slideIndex) => {
          const aktiv = slideIndex === index;
          slide.classList.toggle('is-aktiv', aktiv);
          slide.setAttribute('aria-hidden', aktiv ? 'false' : 'true');
        });
        teamFortschritt.forEach((balken, balkenIndex) => {
          balken.classList.toggle('is-vorbei', balkenIndex < index);
          balken.classList.toggle('is-aktiv', balkenIndex === index);
          balken.style.setProperty('--team-slide-dauer', `${aktuelleDauer}ms`);
        });
      };

      const teamNaechstenSlidePlanen = () => {
        window.clearTimeout(teamSlideTimer);
        teamSlideTimer = window.setTimeout(() => {
          const naechsterSlide = teamAktuellerSlide + 1;
          if (naechsterSlide >= teamSlides.length) {
            teamShowcaseBeenden();
            return;
          }
          teamSlideZeigen(naechsterSlide);
          teamNaechstenSlidePlanen();
        }, teamSlideDauerHolen(teamAktuellerSlide));
      };

      const teamNaechstenStartPlanen = (wartezeit = null) => {
        if (!teamShowcase || !teamSlides.length) return;
        window.clearTimeout(teamTimer);
        const gespeichert = Number(teamSpeicherLesen(teamSpeicherKey)) || 0;
        const letzterStart = gespeichert || (Date.now() - teamIntervall + teamErsterStart);
        const bisZumStart = wartezeit === null
          ? Math.max(1000, teamIntervall - (Date.now() - letzterStart))
          : wartezeit;
        teamTimer = window.setTimeout(() => {
          if (document.visibilityState !== 'visible'
              || (messungenDialog && !messungenDialog.hidden)
              || Date.now() - teamLetzteInteraktion < 15000) {
            teamNaechstenStartPlanen(15000);
            return;
          }
          teamShowcaseStarten();
        }, bisZumStart);
      };

      const teamShowcaseBeenden = () => {
        if (!teamShowcase || teamShowcase.hidden) return;
        window.clearTimeout(teamSlideTimer);
        teamSlideTimer = null;
        teamMelodieStoppen();
        teamShowcase.hidden = true;
        document.body.classList.remove('team-showcase-offen');
        teamSlides.forEach((slide) => {
          slide.classList.remove('is-aktiv');
          slide.setAttribute('aria-hidden', 'true');
        });
        if (teamVorherigerFokus && typeof teamVorherigerFokus.focus === 'function') {
          teamVorherigerFokus.focus();
        }
        teamNaechstenStartPlanen();
      };

      function teamShowcaseStarten() {
        if (!teamShowcase || !teamShowcase.hidden || !teamSlides.length) return;
        window.clearTimeout(teamTimer);
        teamVorherigerFokus = document.activeElement;
        teamSpeicherSchreiben(teamSpeicherKey, Date.now());
        teamSlideZeigen(0);
        teamShowcase.hidden = false;
        document.body.classList.add('team-showcase-offen');
        teamMelodieSpielen();
        if (teamSchliessen) teamSchliessen.focus();
        teamNaechstenSlidePlanen();
      }

      ['pointerdown', 'touchstart', 'keydown'].forEach((ereignis) => {
        document.addEventListener(ereignis, () => {
          teamLetzteInteraktion = Date.now();
        }, { passive: true, capture: true });
      });

      if (teamOeffnen) teamOeffnen.addEventListener('click', teamShowcaseStarten);
      if (teamSchliessen) teamSchliessen.addEventListener('click', teamShowcaseBeenden);
      if (teamTonKnopf) {
        teamTonKnopf.addEventListener('click', async () => {
          const melodieLaeuft = teamMusik && !teamMusik.paused;
          if (teamTonAktiv && melodieLaeuft) {
            teamTonAktiv = false;
            teamSpeicherSchreiben(teamTonKey, 'aus');
            teamMelodieStoppen();
            teamTonAnzeigeAktualisieren();
            return;
          }
          teamTonAktiv = true;
          teamSpeicherSchreiben(teamTonKey, 'an');
          await teamMelodieSpielen();
          teamTonAnzeigeAktualisieren();
        });
      }
      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && teamShowcase && !teamShowcase.hidden) teamShowcaseBeenden();
      });
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible' && teamShowcase && teamShowcase.hidden) {
          teamNaechstenStartPlanen();
        }
      });
      teamTonAnzeigeAktualisieren();
      teamNaechstenStartPlanen();
})();
