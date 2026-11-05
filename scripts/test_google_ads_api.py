from datetime import date, timedelta
from pathlib import Path
import os
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TEST_RUNTIME = tempfile.TemporaryDirectory(prefix="gaertner-google-ads-test-")
TEST_ROOT = Path(TEST_RUNTIME.name)
os.environ["DATABASE_URL"] = ""
os.environ["RENDER"] = "1"
os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "0"
os.environ["DATA_DIR"] = str(TEST_ROOT)
os.environ["SQLITE_DB_PATH"] = str(TEST_ROOT / "app-test.db")
os.environ["UPLOAD_DIR"] = str(TEST_ROOT / "uploads")
os.environ["BACKUP_DIR"] = str(TEST_ROOT / "backups")
os.environ["DELETED_UPLOAD_DIR"] = str(TEST_ROOT / "deleted-uploads")
os.environ["AUTO_BACKUP_ENABLED"] = "0"
os.environ["AUTO_CHANGE_BACKUP_ENABLED"] = "0"
os.environ["LEXWARE_API_KEY"] = ""
for key in (
    "GOOGLE_ADS_DEVELOPER_TOKEN",
    "GOOGLE_ADS_CLIENT_ID",
    "GOOGLE_ADS_CLIENT_SECRET",
    "GOOGLE_ADS_REFRESH_TOKEN",
    "GOOGLE_ADS_CUSTOMER_ID",
    "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
):
    os.environ.pop(key, None)

import app as portal  # noqa: E402


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.headers = {}

    def json(self):
        return self.payload


class FakeGoogleAdsHttp:
    def __init__(self, metric_day, include_shop=False):
        self.metric_day = metric_day
        self.include_shop = include_shop
        self.calls = []

    def post(self, url, data=None, headers=None, json=None, timeout=None):
        self.calls.append({
            "url": url,
            "headers": headers or {},
            "query": (json or {}).get("query", ""),
            "has_form": bool(data),
            "timeout": timeout,
        })
        if url == portal.GOOGLE_TOKEN_URL:
            return FakeResponse({"access_token": "test-access-token"})
        query = (json or {}).get("query", "")
        campaigns = [
            ("1111111111", portal.GOOGLE_ADS_KAMPAGNEN["auto-lackierzentrum"]["kampagne"], 1230000, 6, 120, 2.5),
            ("2222222222", portal.GOOGLE_ADS_KAMPAGNEN["tomorrowworks"]["kampagne"], 450000, 2, 40, 1),
            ("3333333333", portal.GOOGLE_ADS_KAMPAGNEN["autovermietung-mos"]["kampagne"], 990000, 3, 75, 0),
        ]
        if self.include_shop:
            campaigns.append(("24441136966", "Gärtner Shop | umbenannt", 870000, 4, 40, 1))
        if "campaign.status" in query:
            return FakeResponse([{"results": [
                {"campaign": {"id": campaign_id, "name": campaign_name}}
                for campaign_id, campaign_name, *_ in campaigns
            ]}])
        return FakeResponse([{"results": [
            {
                "segments": {"date": self.metric_day},
                "campaign": {"id": campaign_id, "name": campaign_name},
                "metrics": {
                    "costMicros": str(cost_micros),
                    "clicks": str(clicks),
                    "impressions": str(impressions),
                    "conversions": conversions,
                },
            }
            for campaign_id, campaign_name, cost_micros, clicks, impressions, conversions in campaigns
        ]}])


def main():
    portal.app.config["TESTING"] = True
    os.environ.update({
        "GOOGLE_ADS_DEVELOPER_TOKEN": "developer-token-test",
        "GOOGLE_ADS_CLIENT_ID": "oauth-client-test",
        "GOOGLE_ADS_CLIENT_SECRET": "oauth-secret-test",
        "GOOGLE_ADS_REFRESH_TOKEN": "refresh-token-test",
        "GOOGLE_ADS_CUSTOMER_ID": "123-456-7890",
        "GOOGLE_ADS_LOGIN_CUSTOMER_ID": "987-654-3210",
        "GOOGLE_ADS_CAMPAIGN_ID_AUTO_LACKIERZENTRUM": "1111111111",
        "GOOGLE_ADS_CAMPAIGN_ID_TOMORROWWORKS": "2222222222",
        "GOOGLE_ADS_CAMPAIGN_ID_AUTOVERMIETUNG_MOS": "3333333333",
    })
    metric_day = (date.today() - timedelta(days=1)).isoformat()
    fake_http = FakeGoogleAdsHttp(metric_day)
    summary = portal.sync_google_ads_tageswerte(tage=7, http_client=fake_http)
    db = portal.get_db()
    row = db.execute(
        """
        SELECT kosten_cent, klicks, impressionen, conversions, quelle
        FROM google_ads_tageswerte
        WHERE datum=? AND website='auto-lackierzentrum'
        """,
        (metric_day,),
    ).fetchone()
    db.close()
    api_calls = [call for call in fake_http.calls if "googleAds:searchStream" in call["url"]]
    passed = (
        summary["kampagnen"] == 3
        and summary["tageswerte"] == 3
        and row is not None
        and row["kosten_cent"] == 123
        and row["klicks"] == 6
        and row["impressionen"] == 120
        and row["conversions"] == 2.5
        and row["quelle"] == "google_ads_api"
        and len(fake_http.calls) == 3
        and len(api_calls) == 2
        and all(
            call["headers"].get("developer-token") == "developer-token-test"
            and call["headers"].get("login-customer-id") == "9876543210"
            and call["url"].endswith("/v25/customers/1234567890/googleAds:searchStream")
            for call in api_calls
        )
    )
    print(f"[{'OK' if passed else 'FEHLER'}] Google-Ads-OAuth, Header und Tagesimport")

    fake_shop_http = FakeGoogleAdsHttp(metric_day, include_shop=True)
    shop_summary = portal.sync_google_ads_tageswerte(tage=7, http_client=fake_shop_http)
    site = portal.list_google_ads_statistik(7, website="auto-lackierzentrum")
    campaigns = {row["kampagnen_key"]: row for row in site["campaigns"]}
    other_site = portal.list_google_ads_statistik(7, website="autovermietung-mos")
    hidden_site = portal.list_google_ads_statistik(7, ausgeblendete_websites=("auto-lackierzentrum",))
    shop_passed = (
        shop_summary["kampagnen"] == 4 and shop_summary["tageswerte"] == 4
        and "24441136966" in fake_shop_http.calls[-1]["query"]
        and set(campaigns) == {"auto-lackierzentrum", "gaertner-shop"}
        and campaigns["auto-lackierzentrum"]["klicks"] == 6 and campaigns["auto-lackierzentrum"]["kosten_cent"] == 123
        and campaigns["gaertner-shop"]["klicks"] == 4 and campaigns["gaertner-shop"]["kosten_cent"] == 87
        and campaigns["gaertner-shop"]["budget_tag_cent"] == 250
        and all(row["website"] == "auto-lackierzentrum" for row in campaigns.values())
        and site["klicks"] == 10 and site["kosten_cent"] == 210 and site["tagesbudget_cent"] == 750
        and [row["kampagnen_key"] for row in other_site["campaigns"]] == ["autovermietung-mos"]
        and all(row["website"] != "auto-lackierzentrum" for row in hidden_site["campaigns"])
    )
    print(f"[{'OK' if shop_passed else 'FEHLER'}] Separate Shop-Kampagne am gleichen Tag und Website-Filter")
    return 0 if passed and shop_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
