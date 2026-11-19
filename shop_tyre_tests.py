"""Published ADAC overall grades bound to individually checked catalogue variants.

Each match was checked against the original test's model, season, size, load
and speed index, and EU fuel/wet/noise label. EAN and catalogue fingerprint
pin that reviewed match. References are local data, with no request-time IO.
"""
from typing import NamedTuple


class TyreTest(NamedTuple):
    fingerprint: str
    label: tuple[str, str, str]
    year: int
    grade: float
    source_path: str


ADAC_TYRE_TESTS = {
    # CONTINENTAL ALLSEASON CONTACT 2 | CONTINEN AL-CO2 185/65 R15 88 H
    "4019238092028": TyreTest("26132d760b1c1dbe9500c813e2bd033ab17ef538f347b863e3422319ab7a3984",
        ('B', 'B', '71'), 2026, 2.3, "ganzjahresreifen/185-65-r15-2026/continental-allseasoncontact-2-id-5176/"),
    # BARUM QUARTARIS 5 | 225/45 R17 94Y
    "4024063005907": TyreTest("65b656b2cf67b11b43f11645248119726e6626ec653437436bb43580f042aa6c",
        ('C', 'C', '72'), 2025, 3.9, "ganzjahresreifen/225-45-r17/barum-quartaris-5-id-5043/"),
    # CONTINENTAL ALLSEASON CONTACT 2 | CONTINEN AL-CO2 225/45 R17 94 W XL
    "4019238092158": TyreTest("f517245e97888e66a193e404dd0eb0525f7a12f84d1ef1600aa8fffc596a5178",
        ('C', 'B', '71'), 2025, 2.3, "ganzjahresreifen/225-45-r17/continental-allseasoncontact-2-id-5034/"),
    # CONTINENTAL SPORT CONTACT 7 | CONTINEN SP-CO7 225/40ZR18 (92Y) XL
    "4019238078985": TyreTest("2c6926622e02c271bbb055b215d668974dc53e01600ff3c084fa8b0a88046176",
        ('C', 'A', '72'), 2025, 1.8, "sommerreifen/225-40-r18/continental-sportcontact7-id-4986/"),
    # CONTINENTAL PREMIUM CONTACT 7 | 225/50 R17 98Y
    "4019238079098": TyreTest("e56cea6cc1065dc3aac13efbf35788b703f01d1af7f73113ab820c27e106cb28",
        ('C', 'A', '72'), 2026, 1.9, "sommerreifen/225-50-r17-2026/continental-premiumcontact-7-id-5128/"),
    # LINGLONG SPORT MASTER | 225/50 R17 98Y
    "6959956759705": TyreTest("8719db0eff7ddb638019c9f7d6ae7e542220b7b2244823c1013940b8f1d19dc3",
        ('C', 'A', '69'), 2026, 4.2, "sommerreifen/225-50-r17-2026/linglong-sport-master-id-5143/"),
    # BARUM POLARIS 6 | BARUM POL-6 185/65 R15 88 T
    "4024063006843": TyreTest("a6759c1a56cde1c1ea447bf912918402e6b12bcca695f7bf63c9ca011e8d524f",
        ('B', 'C', '71'), 2026, 3.8, "winterreifen/185-65-r15-2026/barum-polaris-6-id-5212/"),
    # CONTINENTAL WINTER CONTACT TS 870 | 185/65 R15 88T
    "4019238038354": TyreTest("11b833fcb2e18ff976db83328a81f2e9afd8c2d2a13ee1782078375f4b11ac55",
        ('C', 'B', '70'), 2026, 2.2, "winterreifen/185-65-r15-2026/continental-wintercontact-ts-870-id-5202/"),
    # BRIDGESTONE BLIZZAK LM-005 | 195/65 R15 91T
    "3286341529111": TyreTest("4087e3f162ca19537f874f57e0b6b2c90b367d7d198be13440bd04dd02bcbb48",
        ('C', 'A', '71'), 2021, 3.0, "winterreifen/195-65-r15/bridgestone-blizzak-lm005-id-4562/"),
    # BRIDGESTONE BLIZZAK LM-005 | 215/60 R16 99H
    "3286341531114": TyreTest("5c1481182eef94e624962075cf4c114e2500f45902aabdf1eda2022c413a387c",
        ('C', 'A', '71'), 2022, 2.7, "winterreifen/215-60-r16-h/bridgestone-blizzak-lm005-id-4668/"),
    # TRIANGLE TW-401 WINTER X | 215/55 R17 98V
    "6959753224369": TyreTest("9ef1fe9f5eb13c44c0b28c3aac9dbddc1430531567867c3f71b2ddf8200cb01f",
        ('D', 'C', '72'), 2024, 4.8, "winterreifen/215-55-r17/triangle-winterx-tw401-id-4905/"),
    # CONTINENTAL WINTER CONTACT TS 870P | 225/40 R18 92V
    "4019238054354": TyreTest("cd7ff903cbf8f8977a2718cd66cd4e94d8a78ada28fa5ee90343b72b22293f5b",
        ('C', 'B', '71'), 2025, 2.5, "winterreifen/225-40-r18/continental-wintercontact-ts-870-p-id-5055/"),
    # SEMPERIT SPEED-GRIP 5 | 225/40 R18 92V
    "4024067003381": TyreTest("f32c052ea776d38eb7e9d1f6e52a700f9d1527d66b49bb21101b558a6bdcdb8c",
        ('D', 'B', '72'), 2025, 3.2, "winterreifen/225-40-r18/semperit-speed-grip-5-id-5063/"),
    # STAR PERFORMER STRATOS UHP | STARPERF ST-UHP 225/40 R18 92 V XL
    "8605068730344": TyreTest("29c5bad09d72c49ab26f7e179df41ea27c3b909dcefc582e2dffcdab56ac28c8",
        ('D', 'C', '72'), 2025, 5.5, "winterreifen/225-40-r18/star-performer-stratos-uhp-id-5076/"),
    # UNIROYAL WINTEREXPERT | 225/40 R18 92V
    "4024068001768": TyreTest("e956baf6a6892de31763ff0e7d3891448556debaf6717b41c2940316a02d677c",
        ('D', 'B', '72'), 2025, 3.0, "winterreifen/225-40-r18/uniroyal-winterexpert-id-5061/"),
    # CONTINENTAL WINTER CONTACT TS 870 | CONTI TS870 225/45 R17 91 H
    "4019238038323": TyreTest("941931775808c0aa3ba51c1fd823caa318f6562ba2c1f1de9311d262b576487c",
        ('C', 'B', '70'), 2023, 2.0, "winterreifen/225-45-r17-h/continental-wintercontact-ts-870-id-4772/"),
    # SEMPERIT SPEED-GRIP 5 | 225/45 R17 91H
    "4024067003336": TyreTest("19ac16617a761aedea81e1b87304ef6780a92d55754f15305d143c9d1b009ce8",
        ('D', 'B', '72'), 2023, 2.6, "winterreifen/225-45-r17-h/semperit-speed-grip-5-id-4777/"),
    # CONTINENTAL WINTER CONTACT TS 870 | 225/50 R17 98V
    "4019238038187": TyreTest("412db1eb21992b0407bdda2fab85bd8070a24f45e12150ac81f3c57345488d0c",
        ('C', 'B', '70'), 2026, 2.3, "winterreifen/225-50-r17-2026/continental-wintercontact-ts-870-id-5219/"),
    # MICHELIN PRIMACY 4+ | 205/55 R16 91V
    "3528704440573": TyreTest("c3cb8e7f6f7ae2e1f72d677bb30bc6a0558199f4a4bc1e8cd10b7c3435a02333",
        ('C', 'A', '69'), 2023, 2.1, "sommerreifen/205-55-r16/michelin-primacy-4plus-id-4692/"),
    # ROTALLA RH01 | ROTALLA RH01 205/55 R16 91 V
    "6958460908586": TyreTest("e8a5744aa12142048600a450862d9f8601678798829f6b5d614bf9b9ec5e6d77",
        ('C', 'B', '69'), 2023, 4.7, "sommerreifen/205-55-r16/rotalla-rh-01-id-4733/"),
    # BARUM BRAVURIS 5 HM | 225/50 R17 98Y
    "4024063001954": TyreTest("5a5cfc388812db1eca8fff4fd11f15f9de3282839ca72963d7ca520a7f779981",
        ('C', 'B', '72'), 2021, 3.4, "sommerreifen/225-50-r17/barum-bravuris-5hm-id-4491/"),
    # SEMPERIT SPEED-LIFE 3 | 225/50 R17 98Y
    "4024067001417": TyreTest("1c1c5fa755c04eda230019f03f413cbd2142bd976a06f2465fe08c83694e69f3",
        ('C', 'B', '72'), 2021, 2.9, "sommerreifen/225-50-r17/semperit-speed-life-3-id-4486/"),
}
