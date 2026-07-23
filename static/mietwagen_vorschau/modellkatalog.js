(() => {
  const normalizeModelName = value => String(value || '')
    .replace(/^(BMW|Mercedes-Benz)\s+/, '')
    .replace(/^1er Hatchback$/, '1er')
    .trim();
  const findImage = (entries, name) => (entries || []).find(entry => normalizeModelName(entry.name) === normalizeModelName(name))?.image || '';
  const vwImage = name => findImage(window.mosVwImageSources, name);
  const bmwImage = name => findImage(window.mosBmwMercedesImageSources?.bmw, name);
  const mercedesImage = name => findImage(window.mosBmwMercedesImageSources?.mercedes, name);

  const catalogs = [
    {
      id: 'audi',
      brand: 'Audi',
      title: 'Audi Wunschmodell auswählen',
      intro: 'Das aktuelle deutsche Audi-Modellprogramm – vom kompakten A1 bis zum elektrischen Gran Turismo. Ausstattung, Antrieb, Lieferzeit und Monatsrate werden individuell beim Handel geprüft.',
      open: true,
      source: 'Audi Deutschland',
      groups: [
        {
          title: 'Kompakt, Limousine & Avant',
          models: [
            ['A1', 'Kompaktklasse', 'audi/a1.webp'],
            ['A3', 'Kompaktklasse', 'audi/a3.webp'],
            ['A5', 'Limousine & Avant', 'audi/a5.webp'],
            ['A6', 'Limousine & Avant', 'audi/a6.webp'],
            ['A8', 'Oberklasse-Limousine', 'audi/a8.webp']
          ]
        },
        {
          title: 'SUV & Crossover',
          models: [
            ['Q2', 'Kompakt-SUV', 'audi/q2.webp'],
            ['Q3', 'SUV', 'audi/q3.webp'],
            ['Q5', 'Premium-SUV', 'audi/q5.webp'],
            ['Q7', 'Großes Premium-SUV', 'audi/q7.webp'],
            ['Q8', 'Premium-SUV-Coupé', 'audi/q8.webp']
          ]
        },
        {
          title: 'Elektro',
          models: [
            ['A6 e-tron', 'Elektro-Limousine & Avant', 'audi/a6-e-tron.webp'],
            ['Q4 e-tron', 'Elektro-SUV', 'audi/q4-e-tron.webp'],
            ['Q6 e-tron', 'Elektro-SUV', 'audi/q6-e-tron.webp'],
            ['e-tron GT', 'Elektro-Gran-Turismo', 'audi/e-tron-gt.webp']
          ]
        },
        {
          title: 'Vorgestellte Neuheiten',
          models: [
            ['A2 e-tron', 'Elektro-Kompaktmodell · angekündigt', 'audi/a2-e-tron.webp', 'upcoming'],
            ['Nuvolari', 'Vorgestellt · Bestellstart wird geprüft', 'audi/nuvolari.webp', 'upcoming']
          ]
        }
      ]
    },
    {
      id: 'ford',
      brand: 'Ford',
      title: 'Ford Wunschmodell auswählen',
      intro: 'Das aktuelle deutsche Ford-Programm mit Pkw, Elektro-SUV, Tourneo-Familie und ausgewählten Ford-Pro-Modellen. Die konkrete Beschaffung wird nach deiner Anfrage individuell geprüft.',
      source: 'Ford Deutschland',
      groups: [
        {
          title: 'SUV, Crossover & Elektro',
          models: [
            ['Puma Gen-E', 'Elektro-Kompakt-SUV', 'https://www.ford.de/content/dam/guxeu/global-shared/vehicle-images/puma-gen-e/ford-puma_gen_e-eu-16x9-768x432-showroom-heroimage.png.renditions.original.png'],
            ['Explorer', 'Elektro-SUV', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/ford-de-new_ford_explorer-2160x1215-16x9-showroom.png.renditions.original.png'],
            ['Capri', 'Elektro-Crossover', 'https://www.ford.de/content/dam/guxeu/global-shared/vehicle-images/capri/ford-capri-eu-cx740l_showroom-16x9-768x432.png.renditions.original.png'],
            ['Kuga', 'SUV · Hybrid & Plug-in-Hybrid', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/kuga/ford-new_kuga_mca-de-16x9-1600x900-dark-green.png.renditions.original.png'],
            ['Puma', 'Mild-Hybrid-Crossover', 'https://www.ford.de/content/dam/guxeu/global-shared/vehicle-images/puma-mca/ford-puma-eu-mca_showroom-16x9-768x432.png.renditions.original.png'],
            ['Mustang Mach-E', 'Elektro-Performance-SUV', 'https://www.ford.de/content/dam/guxeu/fi/showroom-pv/ford-mustang_mach_e-fi-getImage_1-16x9-768x432-red-mustang-mach-e-vehicle-image.png.renditions.original.png'],
            ['Bronco', 'Offroad-SUV', 'https://www.ford.de/content/dam/guxeu/de/gforce-images/bronco/ford-bronco-de-1b9d35ad_a12c_33a1_9681_a728aeb48132-768x432-white-bronco.png.renditions.original.png']
          ]
        },
        {
          title: 'Sport & Kompakt',
          models: [
            ['Mustang', 'V8-Sportwagen', 'https://www.ford.de/content/dam/guxeu/de/cars/mustang/ford-mustang-de-new_mustang_showroom-768x432-new_mustang.png.renditions.original.png'],
            ['Focus', 'Kompaktklasse · nur Restbestand', 'https://www.ford.de/content/dam/guxeu/global-shared/vehicle-images/focus/ford-focus-eu-16x9-768x432-red-focus.png.renditions.original.png', 'stock']
          ]
        },
        {
          title: 'Tourneo & Freizeit',
          models: [
            ['Tourneo Courier', 'Kompakter Freizeit-Van', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/tourneo-courier/ford-new_tourneo_courier-de-678x381-white-tourneo-courier-new.png.renditions.original.png'],
            ['Tourneo Connect', 'Flexibler Familien-Van', 'https://www.ford.de/content/dam/guxeu/de/manually-authored-vehicles-images/tourneo-connect/ford-de-tourneo_connect-TourneoConnect-16x9-768x432-new-image.png.renditions.original.png'],
            ['Tourneo Custom', 'Großraum-Van', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/tourneo-custom/ford-touneo_custom-de-16x9-1600x900-new.png.renditions.original.png'],
            ['Nugget', 'Camper & Freizeitfahrzeug', 'https://www.ford.de/content/dam/guxeu/global-shared/vehicle-images/transit-custom-nugget/ford-transitnugget-eu-Nugget_19_Ext_Awning__0572-1_V9_Ext-16x9-767x431-showroom.jpg.renditions.original.png']
          ]
        },
        {
          title: 'Ford Pro Pick-ups',
          models: [
            ['Ranger', 'Pick-up', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/new-ranger/ford-ranger-de-RANGER-16x9-768x432-new-ranger.renditions.original.png'],
            ['Ranger Raptor', 'Performance-Pick-up', 'https://www.ford.de/content/dam/guxeu/de/vehicle-images/ranger-raptor/ford-ranger_raptor-de-RANGER_RAPTOR-16x9-768x432-new-ranger-raptor.renditions.original.png']
          ]
        }
      ]
    },
    {
      id: 'byd',
      brand: 'BYD',
      title: 'BYD Wunschmodell auswählen',
      intro: 'Das aktuelle deutsche BYD-Programm umfasst vollelektrische Modelle und Plug-in-Hybride mit DM-i-Technik. Lieferzeit, Konfiguration und Monatsrate werden individuell geprüft.',
      source: 'BYD Deutschland',
      groups: [
        {
          title: 'Vollelektrische Modelle',
          models: [
            ['ATTO 2', 'Elektro-Kompakt-SUV', 'https://www.byd.com/material/byd-site/eu/electric-cars/BYD_ATTO_2_EV-card.jpg'],
            ['ATTO 3 EVO', 'Elektro-SUV', 'https://www.byd.com/material/byd-site/eu/electric-cars/atto-3-evo-card.webp'],
            ['DOLPHIN', 'Elektro-Kompaktklasse', 'https://www.byd.com/material/byd-site/eu/electric-cars/dolphin-ev-card.png'],
            ['DOLPHIN SURF', 'Elektro-City-Car', 'https://www.byd.com/material/byd-site/eu/electric-cars/dolphin-surf-ev-card.jpg'],
            ['SEAL', 'Elektro-Limousine', 'https://www.byd.com/material/byd-site/eu/electric-cars/seal-ev-card.png'],
            ['SEALION 7', 'Elektro-SUV-Coupé', 'https://www.byd.com/material/byd-site/eu/electric-cars/sealion-7-ev-card.jpg'],
            ['TANG', 'Elektro-SUV · 7-Sitzer', 'https://www.byd.com/material/byd-site/eu/electric-cars/tang-2024-ev-card.jpg']
          ]
        },
        {
          title: 'Plug-in-Hybrid mit DM-i',
          models: [
            ['ATTO 2 DM-i', 'Plug-in-Hybrid-SUV', 'https://www.byd.com/material/byd-site/eu/hybrid-cars/atto2-dm-i-hybrid-card.png'],
            ['DOLPHIN G DM-i', 'Plug-in-Hybrid-Kompaktmodell', 'https://www.byd.com/material/byd-site/de/product/no-background-product-images/hybrid_byd_dolphin_g.webp'],
            ['SEAL U DM-i', 'Plug-in-Hybrid-SUV', 'https://www.byd.com/material/byd-site/eu/hybrid-cars/Sealudm-i-hybrid-card.png'],
            ['SEAL 6 DM-i Touring', 'Plug-in-Hybrid-Kombi', 'https://www.byd.com/material/byd-site/eu/hybrid-cars/seal-6-dm-i-touring-hybrid-card.jpg']
          ]
        }
      ]
    },
    {
      id: 'bmw',
      brand: 'BMW',
      title: 'BMW Wunschmodell auswählen',
      intro: 'Das aktuelle deutsche BMW-Modellprogramm, nach Baureihen zusammengefasst. M-Performance-Motorisierungen sind nicht doppelt aufgeführt; eigenständige BMW-M-Familien bleiben auswählbar.',
      source: 'BMW Deutschland',
      groups: [
        {
          title: 'Kompakt & 2er',
          models: [
            ['1er Hatchback', 'Kompaktklasse', bmwImage('1er Hatchback')],
            ['2er Active Tourer', 'Kompakt-Van', bmwImage('2er Active Tourer')],
            ['2er Coupé', 'Sportcoupé', bmwImage('2er Coupé')],
            ['2er Gran Coupé', 'Kompakte Limousine', bmwImage('2er Gran Coupé')],
            ['M2 Coupé', 'M Performance-Coupé', bmwImage('M2 Coupé')]
          ]
        },
        {
          title: '3er & 4er',
          models: [
            ['3er Limousine', 'Mittelklasse-Limousine', bmwImage('3er Limousine')],
            ['3er Touring', 'Mittelklasse-Kombi', bmwImage('3er Touring')],
            ['i3 Limousine', 'Neue Elektro-Limousine', bmwImage('i3 Limousine'), 'upcoming'],
            ['M3 Limousine', 'M Performance-Limousine', bmwImage('M3 Limousine')],
            ['M3 Touring', 'M Performance-Kombi', bmwImage('M3 Touring')],
            ['4er Coupé', 'Premium-Coupé', bmwImage('4er Coupé')],
            ['4er Gran Coupé', 'Viertüriges Coupé', bmwImage('4er Gran Coupé')],
            ['4er Cabrio', 'Premium-Cabriolet', bmwImage('4er Cabrio')],
            ['i4 Gran Coupé', 'Elektro-Gran-Coupé', bmwImage('i4 Gran Coupé')],
            ['M4 Coupé', 'M Performance-Coupé', bmwImage('M4 Coupé')],
            ['M4 Cabrio', 'M Performance-Cabriolet', bmwImage('M4 Cabrio')]
          ]
        },
        {
          title: '5er & Oberklasse',
          models: [
            ['5er Limousine', 'Business-Limousine', bmwImage('5er Limousine')],
            ['5er Touring', 'Business-Kombi', bmwImage('5er Touring')],
            ['i5 Limousine', 'Elektro-Business-Limousine', bmwImage('i5 Limousine')],
            ['i5 Touring', 'Elektro-Business-Kombi', bmwImage('i5 Touring')],
            ['M5 Limousine', 'M Performance-Limousine', bmwImage('M5 Limousine')],
            ['M5 Touring', 'M Performance-Kombi', bmwImage('M5 Touring')],
            ['7er Limousine', 'Luxuslimousine', bmwImage('7er Limousine')],
            ['i7 Limousine', 'Elektro-Luxuslimousine', bmwImage('i7 Limousine')]
          ]
        },
        {
          title: 'X-Modelle, Roadster & XM',
          models: [
            ['X1', 'Kompakt-SUV', bmwImage('X1')],
            ['iX1', 'Elektro-Kompakt-SUV', bmwImage('iX1')],
            ['X2', 'Kompakt-SUV-Coupé', bmwImage('X2')],
            ['iX2', 'Elektro-SUV-Coupé', bmwImage('iX2')],
            ['X3', 'Premium-SUV', bmwImage('X3')],
            ['iX3', 'Elektro-Premium-SUV', bmwImage('iX3')],
            ['X5', 'Großes Premium-SUV', bmwImage('X5')],
            ['X5 M', 'Neue M-Hybrid-Version', bmwImage('X5 M'), 'upcoming'],
            ['iX5', 'Angekündigtes Elektro-SUV', bmwImage('iX5'), 'upcoming'],
            ['X6', 'Premium-SUV-Coupé', bmwImage('X6')],
            ['X6 M', 'M Performance-SUV-Coupé', bmwImage('X6 M')],
            ['X7', 'Luxus-SUV · 7-Sitzer', bmwImage('X7')],
            ['iX', 'Elektro-Luxus-SUV', bmwImage('iX')],
            ['Z4 Roadster', 'Roadster', bmwImage('Z4 Roadster')],
            ['XM', 'M High-Performance-SUV', bmwImage('XM')]
          ]
        }
      ]
    },
    {
      id: 'volkswagen',
      brand: 'Volkswagen',
      title: 'Volkswagen Wunschmodell auswählen',
      intro: 'Die aktuell im deutschen Volkswagen-Konfigurator geführten Pkw-Modellfamilien. Sondermodelle und einzelne Motorisierungen werden nicht doppelt gezählt.',
      source: 'Volkswagen Deutschland',
      groups: [
        {
          title: 'Kleinwagen, Kompakt & Crossover',
          models: [
            ['Polo', 'Kleinwagen', vwImage('Polo')],
            ['Golf', 'Kompaktklasse · Verbrenner & Hybrid', vwImage('Golf')],
            ['Taigo', 'Crossover-Coupé', vwImage('Taigo')],
            ['T-Cross', 'Kompakt-SUV', vwImage('T-Cross')],
            ['T-Roc', 'Kompakt-SUV', vwImage('T-Roc')],
            ['T-Roc Cabriolet', 'Cabrio-SUV', vwImage('T-Roc Cabriolet')]
          ]
        },
        {
          title: 'SUV',
          models: [
            ['Tiguan', 'SUV · Verbrenner & Hybrid', vwImage('Tiguan')],
            ['Tayron', 'Großes SUV · Verbrenner & Hybrid', vwImage('Tayron')],
            ['Touareg', 'Oberklasse-SUV · nur Lagerfahrzeuge', vwImage('Touareg'), 'stock']
          ]
        },
        {
          title: 'Vollelektrische ID. Modelle',
          models: [
            ['ID. Polo', 'Elektro-Kleinwagen', vwImage('ID. Polo')],
            ['ID.3 Neo', 'Elektro-Kompaktklasse', vwImage('ID.3 Neo')],
            ['ID. Cross', 'Elektro-Kompakt-SUV', vwImage('ID. Cross')],
            ['ID.4', 'Elektro-SUV', vwImage('ID.4')],
            ['ID.5', 'Elektro-SUV-Coupé', vwImage('ID.5')],
            ['ID.7', 'Elektro-Limousine', vwImage('ID.7')],
            ['ID.7 Tourer', 'Elektro-Kombi', vwImage('ID.7 Tourer')]
          ]
        },
        {
          title: 'Kombi',
          models: [
            ['Golf Variant', 'Kompakt-Kombi', vwImage('Golf Variant')],
            ['Passat', 'Mittelklasse-Kombi · Verbrenner & Hybrid', vwImage('Passat')]
          ]
        }
      ]
    },
    {
      id: 'mercedes',
      brand: 'Mercedes-Benz',
      title: 'Mercedes-Benz Wunschmodell auswählen',
      intro: 'Das aktuelle deutsche Mercedes-Benz-Programm, nach Modellfamilien gebündelt. AMG- und Maybach-Familien bleiben dort separat, wo sie vom Hersteller als eigenständige Baureihe geführt werden.',
      source: 'Mercedes-Benz Deutschland',
      groups: [
        {
          title: 'Limousinen',
          models: [
            ['CLA', 'Kompakt-Limousine · Elektro & Hybrid', mercedesImage('CLA')],
            ['C-Klasse Limousine', 'Mittelklasse-Limousine', mercedesImage('C-Klasse Limousine')],
            ['EQE Limousine', 'Elektro-Business-Limousine', mercedesImage('EQE Limousine')],
            ['EQS Limousine', 'Elektro-Luxuslimousine', mercedesImage('EQS Limousine')],
            ['E-Klasse Limousine', 'Business-Limousine', mercedesImage('E-Klasse Limousine')],
            ['S-Klasse', 'Luxuslimousine', mercedesImage('S-Klasse')],
            ['S-Klasse Lang', 'Luxuslimousine · Langversion', mercedesImage('S-Klasse Lang')],
            ['Mercedes-Maybach S-Klasse', 'Maybach-Luxuslimousine', mercedesImage('Mercedes-Maybach S-Klasse')]
          ]
        },
        {
          title: 'SUV & Geländewagen',
          models: [
            ['EQA', 'Elektro-Kompakt-SUV', mercedesImage('EQA')],
            ['EQE SUV', 'Elektro-Premium-SUV', mercedesImage('EQE SUV')],
            ['EQS SUV', 'Elektro-Luxus-SUV', mercedesImage('EQS SUV')],
            ['GLA', 'Kompakt-SUV', mercedesImage('GLA')],
            ['GLB', 'Kompakt-SUV · bis 7 Sitze', mercedesImage('GLB')],
            ['GLC', 'Premium-SUV', mercedesImage('GLC')],
            ['GLC Coupé', 'Premium-SUV-Coupé', mercedesImage('GLC Coupé')],
            ['GLE', 'Großes Premium-SUV', mercedesImage('GLE')],
            ['GLE Coupé', 'Großes SUV-Coupé', mercedesImage('GLE Coupé')],
            ['GLS', 'Luxus-SUV · 7-Sitzer', mercedesImage('GLS')],
            ['G-Klasse', 'Geländewagen · Elektro & Verbrenner', mercedesImage('G-Klasse')],
            ['Mercedes-Maybach EQS SUV', 'Elektrisches Maybach-Luxus-SUV', mercedesImage('Mercedes-Maybach EQS SUV')],
            ['Mercedes-Maybach GLS', 'Maybach-Luxus-SUV · angekündigt', mercedesImage('Mercedes-Maybach GLS'), 'upcoming']
          ]
        },
        {
          title: 'T-Modelle & Kompaktwagen',
          models: [
            ['CLA Shooting Brake', 'Sportkombi · Elektro & Hybrid', mercedesImage('CLA Shooting Brake')],
            ['C-Klasse T-Modell', 'Mittelklasse-Kombi', mercedesImage('C-Klasse T-Modell')],
            ['C-Klasse T-Modell All-Terrain', 'All-Terrain-Kombi', mercedesImage('C-Klasse T-Modell All-Terrain')],
            ['E-Klasse T-Modell', 'Business-Kombi', mercedesImage('E-Klasse T-Modell')],
            ['E-Klasse T-Modell All-Terrain', 'All-Terrain-Business-Kombi', mercedesImage('E-Klasse T-Modell All-Terrain')],
            ['A-Klasse Kompaktlimousine', 'Kompaktklasse', mercedesImage('A-Klasse Kompaktlimousine')],
            ['B-Klasse', 'Kompakt-Van', mercedesImage('B-Klasse')]
          ]
        },
        {
          title: 'Coupés, Cabriolets & Roadster',
          models: [
            ['CLE Coupé', 'Premium-Coupé', mercedesImage('CLE Coupé')],
            ['Mercedes-AMG GT Coupé', 'AMG-Sportwagen', mercedesImage('Mercedes-AMG GT Coupé')],
            ['Mercedes-AMG GT 4-Türer Coupé', 'Elektrisches Performance-Coupé', mercedesImage('Mercedes-AMG GT 4-Türer Coupé')],
            ['CLE Cabriolet', 'Premium-Cabriolet', mercedesImage('CLE Cabriolet')],
            ['Mercedes-AMG SL Roadster', 'AMG-Roadster', mercedesImage('Mercedes-AMG SL Roadster')],
            ['Mercedes-Maybach SL Monogram Series', 'Maybach-Luxus-Roadster', mercedesImage('Mercedes-Maybach SL Monogram Series')]
          ]
        },
        {
          title: 'Grand Limousine, Vans & Reisemobile',
          models: [
            ['VLE', 'Elektrische Grand Limousine', mercedesImage('VLE')],
            ['EQV', 'Elektro-Großraumlimousine', mercedesImage('EQV')],
            ['V-Klasse', 'Großraumlimousine', mercedesImage('V-Klasse')],
            ['Marco Polo', 'Reisemobil', mercedesImage('Marco Polo')],
            ['Marco Polo Horizon', 'Freizeit-Van', mercedesImage('Marco Polo Horizon')]
          ]
        }
      ]
    }
  ];

  const root = document.getElementById('weitere-marken');
  if (!root) return;

  const countModels = catalog => catalog.groups.reduce((sum, group) => sum + group.models.length, 0);
  const modelCard = (brand, model) => {
    const [name, type, image, status = ''] = model;
    const suffix = status === 'upcoming' ? ' (angekündigt)' : status === 'stock' ? ' (Bestand)' : '';
    const label = brand === 'Mercedes-Benz' && /^Mercedes-(AMG|Maybach)\b/.test(name) ? name : `${brand} ${name}`;
    const value = `${label}${suffix}`;
    const picture = image ? `<img src="${image}" alt="${label} – Herstellerabbildung" loading="lazy" decoding="async">` : '';
    return `<button class="model-button${status ? ` ${status}` : ''}" data-abo-model="${value}">${picture}<strong>${name}</strong><span>${type}</span></button>`;
  };

  const brandOrder = ['Audi', 'BMW', 'Volkswagen', 'Ford', 'Mercedes-Benz', 'BYD'];
  catalogs.sort((a, b) => brandOrder.indexOf(a.brand) - brandOrder.indexOf(b.brand));

  root.innerHTML = catalogs.map(catalog => {
    const count = countModels(catalog);
    const groups = catalog.groups.map(group => `<div class="model-group"><h4>${group.title}</h4><div class="model-grid">${group.models.map(model => modelCard(catalog.brand, model)).join('')}</div></div>`).join('');
    return `<details class="catalog-toggle" id="marke-${catalog.id}"${catalog.open ? ' open' : ''}><summary><span><span class="catalog-summary-kicker">${catalog.brand} Modellprogramm</span><strong>${catalog.brand} entdecken</strong></span><span class="catalog-summary-meta"><span>${count} Modellfamilien</span><i class="catalog-summary-icon" aria-hidden="true"></i></span></summary><div class="model-catalog"><div class="model-catalog-head"><div><div class="kicker">${catalog.brand} Modellprogramm</div><h3>${catalog.title}</h3></div><p>${catalog.intro}</p></div>${groups}<p class="legal-note">Autovermietung MOS ist kein ${catalog.brand}-Vertragshändler. Herstellerabbildungen: ${catalog.source}, zur Modellorientierung. Modellnamen beschreiben ausschließlich den Kundenwunsch. Verfügbarkeit und Beschaffung nur nach Händlerprüfung.</p></div></details>`;
  }).join('');
})();
