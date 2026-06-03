from __future__ import annotations

import pandas as pd
import numpy as np
from pyspark.sql import functions as F
import seaborn as sns
import math
import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Set, Dict, Any


def load_data(
    schema: str = 'dev.mohit_gangwani',
    table_part: str = 'ad_labeling_final_features_unbinned_',
    date_to_run: str = None
):
    date_to_run = date_to_run.replace('-', '')
    df = spark.table(f"{schema}.{date_to_run}").toPandas()
    df.fillna(0, inplace=True)
    df.rename(
        columns={
            "top5_dma_mix_ratio": "mix_ratio",
            "significant_dma_count_05": "sig_dma_count",
            "significant_region_count_05": "sig_region_count",
            "top_region_mix": "region_mix"
        },
        inplace=True,
    )
    metrics = [
        "coverage_score",
        "entropy_norm",
        "sig_dma_count",
        "mix_ratio",
        "dma_in_90",
        "region_count_90",
        "sig_region_count",
        "region_mix",
    ]
    for col in metrics:
        df.loc[:, col] = df[col].astype(float)
    return df


US_STATE_ABBR = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "dc", "de", "fl",
    "ga", "hi", "ia", "id", "il", "in", "ks", "ky", "la", "ma", "md",
    "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd", "ne", "nh",
    "nj", "nm", "nv", "ny", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "va", "vt", "wa", "wi", "wv", "wy"
}

STATE_CAPITALS = {
    "montgomery", "juneau", "phoenix", "little rock", "sacramento", "denver", "hartford", "dover", "tallahassee", "atlanta",
    "honolulu", "boise", "springfield", "indianapolis", "des moines", "topeka", "frankfort", "baton rouge", "augusta",
    "annapolis", "boston", "lansing", "saint paul", "st paul", "jackson", "jefferson city", "helena", "lincoln", "carson city",
    "concord", "trenton", "santa fe", "albany", "raleigh", "bismarck", "columbus", "oklahoma city", "salem", "harrisburg",
    "providence", "columbia", "pierre", "nashville", "austin", "salt lake city", "montpelier", "richmond", "olympia",
    "charleston", "madison", "cheyenne", "washington"
}

AMBIG_CAPITALS = {
    "washington", "madison", "jackson", "lincoln", "springfield", "columbus", "boston", "augusta", "salem", "richmond"
}

US_STATE_FULL = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts",
    "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia", "dc", "d.c.", "wdc", "washington d.c.", "washington dc"
}

NATIONAL_MARKER_PHRASES = [
    "usa", "u s a", "u.s.a", "united states", "nationwide", "national", "all 50", "all fifty", "america"
]

NETWORK_TOKENS = {
    "abc", "cbs", "nbc", "pbs", "tnt", "tbs", "fox", "cw", "own", "espn", "hbo", "amc", "fx", "syfy", "usa", "yes", "hbo", "paramount", "netflix", "peacock",
    "trutv"
}
LOCAL_STATION_RE = re.compile(r"\b(abc|cbs|nbc|fox|pbs|cw)\s+\d+\b")

DEALER_TOKENS = {"dealer", "dealers", "dealership", "motors", "auto", "autos", "automotive"}

REGION_PHRASES = {
    'abilene', 'ada', 'akron', 'albany', 'albuquerque', 'alexandria', 'alpena',
    'altoona', 'amarillo', 'ames', 'anchorage', 'appleton', 'asheville',
    'atlanta', 'auburn', 'austin', 'bakersfield', 'baltimore', 'bangor',
    'baton rouge', 'bay city', 'beaumont', 'beckley', 'bend', 'billings',
    'biloxi', 'binghamton', 'birmingham', 'bloomington', 'bluefield', 'boise',
    'boston', 'bowling green', 'bozeman', 'bryan', 'bsmrck', 'buffalo',
    'burlington', 'butte', 'cadillac', 'cape girard', 'casper', 'cedar rapids',
    'champaign', 'charleston', 'charlotte', 'charlottesville', 'chattanooga',
    'cheyenne', 'chicago', 'chico', 'cincinnati', 'clarksburg', 'cleveland',
    'colorado springs', 'columbia', 'columbus', 'corpus christi', 'dallas',
    'davenport', 'dayton', 'daytona bch', 'dcknsn', 'decatur', 'denver',
    'des moines', 'detroit', 'dothan', 'dublin', 'duluth', 'durham',
    'eau claire', 'el centro', 'el dorado', 'el paso', 'elkhart', 'elmira',
    'erie', 'eugene', 'eureka', 'evansville', 'fairbanks', 'fargo',
    'fayetvlle', 'flint', 'florence', 'fresno', 'ft walt', 'ft lauderdale',
    'ft myers', 'ft pierce', 'ft smith', 'ft wayne', 'ft worth', 'gainesville',
    'glendive', 'grand junction', 'grand rapids', 'great falls', 'green bay',
    'greensboro', 'greenville', 'greenvll', 'greenwood', 'gulfport',
    'hannibal', 'harlingen', 'harrisburg', 'harrisonburg', 'harsbg', 'hartford',
    'hattiesburg', 'helena', 'holyoke', 'honolulu', 'houston', 'huntington',
    'huntsville', 'hutchinson', 'idaho falls', 'indianapolis', 'jackson',
    'jacksonville', 'jefferson city', 'johnstown', 'jonesboro', 'joplin',
    'juneau', 'kalmzoo', 'kansas city', 'keokuk', 'kirksville',
    'klamath falls', 'knoxville', 'la crosse', 'lafayette', 'lake charles',
    'lansing', 'laredo', 'las vegas', 'laurel', 'lawton', 'lexington', 'lima',
    'hastings', 'little rock', 'lncstr', 'longview', 'los angeles', 'louisville',
    'lubbock', 'lynchburg', 'macon', 'madison', 'mankato', 'marquette',
    'mason city', 'medford', 'memphis', 'meridian', 'miami', 'midland', 'milwaukee',
    'minneapolis', 'minot', 'missoula', 'mobile', 'modesto', 'moline', 'monroe',
    'monterey', 'montgomery', 'montrose', 'myrtle beach', 'naples', 'nashville',
    'new bedford', 'new haven', 'new orleans', 'new york', 'newpt nws', 'norfolk', 
    'north platte', 'oak hill', 'oakland', 'odessa', 'oklahoma city', 'omaha', 
    'orlando', 'ottumwa', 'paducah', 'palm springs', 'panama city', 'parkersburg', 
    'pasco', 'pensacola', 'peoria', 'petersburg', 'philadelphia', 'phoenix', 
    'pine bluff', 'pittsburg', 'pittsburgh', 'plattsburgh', 'pocatello', 
    'port arthur', 'portland', 'portsmth', 'presque isle', 'providence', 
    'pueblo', 'quincy', 'raleigh', 'rapid city', 'redding', 'reno', 'rhinelander', 
    'richland', 'richmond', 'riverton', 'roanoke', 'rochester', 'rochestr', 
    'rockford', 'sacramento', 'saginaw', 'salem', 'salinas', 'salisbury', 
    'salt lake city', 'san angelo', 'san antonio', 'san diego', 'san francisco', 
    'san jose', 'sanluob', 'sanmar', 'santa fe', 'santabarbra', 'sarasota', 
    'savannah', 'schenectady', 'scottsbluff', 'scranton', 'seattle', 'selma', 
    'sherman', 'shreveport', 'sioux city', 'sioux falls', 'south bend', 'spart', 
    'spokane', 'springdale', 'springfield', 'st colge', 'st joseph', 'st louis', 
    'st paul', 'st pete', 'steubenville', 'stockton', 'superior', 'sweetwater', 
    'syracuse', 'tacoma', 'tallahassee', 'tampa', 'temple', 'terre haute', 
    'thomasville', 'toledo', 'topeka', 'traverse city', 'tri cities', 
    'tucson', 'tulsa', 'tupelo', 'twin falls', 'tyler', 'utica', 'victoria', 
    'visalia', 'waco', 'washington', 'waterloo', 'watertown', 'wausau', 
    'west palm beach', 'west point', 'weston', 'wheeling', 'wichita', 
    'wichita falls', 'wilkes barre', 'wilmington', 'yakima', 'york', 'youngstown', 
    'yuma', 'zanesville', 'sierra vista', 'prescott', 'las cruces', 'canton', 
    'big bend', 'ozarks', 'quad cities', 'hamptons', 'texarkana', 'michiana', 
    'four corners', 'golf coast', 'florida keys', 'forgotten coast', 'acadia', 
    'peninsula', 'south bay', 'north bay', 'central coast', 'tri counties', 
    'chino valley', 'east bay', 'bay area', 'dmv', 'tri state', 'tri-state', 
    'inland empire', 'twin cities', 'upstate', 'socal', 'so cal', 'norcal', 
    'nor cal', 'lone start', 'central pa', 'allentown', 'amherst town', 
    'ann arbor', 'annapolis', 'anniston', 'athens', 'atlantic city', 'augusta', 
    'barnstable town', 'battle creek', 'bellingham', 'bismarck', 'blacksburg', 
    'boise city', 'boulder', 'bremerton', 'bridgeport', 'brownsville', 'brunswick', 
    'cape coral', 'cape girardeau', 'carson city', 'chambersburg', 'clarksville', 
    'coeur dalene', 'college station', 'concord', 'corvallis', 'crestview', 
    'dalton', 'daphne', 'deltona', 'dover', 'dubuque', 'eagle pass', 
    'elizabethtown', 'enid', 'farmington', 'fayetteville', 'flagstaff', 
    'fond du lac', 'fort collins', 'fort smith', 'fort wayne', 'frankfort', 
    'fredericksburg', 'gadsden', 'gettysburg', 'glens falls', 'goldsboro', 
    'grand forks', 'grand island', 'grants pass', 'greeley', 'hagerstown', 
    'hammond', 'hanford', 'hickory', 'hilton head island', 'hinesville', 
    'homosassa springs', 'hot springs', 'houma', 'iowa city', 'ithaca', 
    'janesville', 'johnson city', 'kahului', 'kalamazoo', 'kankakee', 
    'kennewick', 'kenosha', 'killeen', 'kingsport', 'kingston', 'kiryas joel', 
    'kokomo', 'lake havasu city', 'lakeland', 'lancaster', 'lawrence', 
    'lebanon', 'lewiston', 'lexington park', 'lincoln', 'logan', 'manchester', 
    'manhattan', 'mansfield', 'mcallen', 'merced', 'michigan city', 'montpelier', 
    'morgantown', 'morristown', 'mount vernon', 'muncie', 'muskegon', 'napa', 
    'niles', 'north port', 'norwich', 'ocala', 'ogden', 'olympia', 'oshkosh', 
    'owensboro', 'oxnard', 'palm bay', 'pierre', 'pinehurst', 'pittsfield', 
    'port st lucie', 'prescott valley', 'provo', 'punta gorda', 'racine', 
    'reading', 'riverside', 'rocky mount', 'rome', 'sacramento', 'saint paul', 
    'san luis obispo', 'sandusky', 'santa cruz', 'santa maria', 'santa rosa', 
    'sebastian', 'sebring', 'sheboygan', 'slidell', 'spartanburg', 'st cloud', 
    'st george', 'state college', 'staunton', 'sumter', 'trenton', 'tuscaloosa', 
    'urban honolulu', 'valdosta', 'vallejo', 'vineland', 'virginia beach', 
    'walla walla', 'warner robins', 'waterbury', 'weirton', 'wenatchee', 
    'wildwood', 'williamsport', 'winchester', 'winston', 'worcester', 'yuba city'
}

AUTO_MODEL_PLACE_WORDS = {
    "colorado", "tahoe", "kona", "santa fe", "atlas", "durango", "canyon", 
    "sierra", "terrain", "tacoma", "suburban", "acadia", "canyon", "savana", 
    "sierra", "yukon", "santa fe", "santa cruz", "tucson", "telluride", 
    "durango", "grand cherokee", "sequoia", "malibu", "murano", "palisade", "sorento"
}

AUTO_BRAND_TOKENS = {
    "chevy", "chevrolet", "gmc", "ford", "toyota", "honda", "hyundai", "jeep", 
    "kia", "nissan", "subaru", "vw", "volkswagen", "dodge", "chrysler"
}
AUTO_DEALER_TOKENS = {
    "dealer", "dealers", "dealership", "auto", "autos", "automotive", "motors"
}
AUTO_DEALER_GENERIC_WORDS = {
    "promo", "offer", "offers", "sale", "sales", "event", "events",
    "lease", "financing", "apr", "msrp", "special", "specials",
    "inventory", "clearance", "certified", "new", "used"
}

# Phone parsing
TOLL_FREE_AREAS = {"800", "888", "877", "866", "855", "844", "833", "822"}
PHONE_RE = re.compile(
    r"(?:\+?1[\s\-\.])?(?:\(?(\d{3})\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4}"
)

ELIGIBLE_FORCE_LOCAL = {
    "services",
    "health",
    "gov_other",
    "restaurants_table",
    "auto_dealer",
    "auto_other",
    "entertainment",
    "travel",
    "gov_pol_nat",
}

DISABLE_BRAND_NLP = {"excluded"}

def _safe_log(x: float) -> float:
    return math.log(x) if x > 0 else 0.0


def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = s.lower().strip()
    s = s.replace(" & ", " and ")
    s = re.sub(r"[.,:/()'\"]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(s: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", norm_text(s))


def compile_re(items: Set[str]) -> Optional[re.Pattern]:
    if not items:
        return None
    esc = sorted((re.escape(x) for x in items), key=len, reverse=True)
    return re.compile(r"\b(?:%s)\b" % "|".join(esc))



class WeeklyAdEligibility:
    def __init__(
        self,
        impression_col="total_impressions",
        train_mass_cutoff=0.999,  # 99.9% for training
        train_quantile=0.50,
        score_quantile=0.40,
        defer_quantile=0.333,
        min_train_floor=1000,
        min_score_floor=500,
    ):
        self.impression_col = impression_col
        self.train_mass_cutoff = train_mass_cutoff
        self.train_q = train_quantile
        self.score_q = score_quantile
        self.defer_q = defer_quantile
        self.min_train_floor = min_train_floor
        self.min_score_floor = min_score_floor

    def _mass_restricted_universe(self, df: pd.DataFrame, cutoff: float):
        df = df.sort_values(self.impression_col, ascending=False)
        total = df[self.impression_col].sum()
        df["cum_mass"] = df[self.impression_col].cumsum() / total
        return df[df["cum_mass"] <= cutoff]

    def compute_thresholds(self, df: pd.DataFrame) -> dict:
        df = df.copy()

        # ---- TRAIN thresholds (mass-restricted) ----
        train_universe = self._mass_restricted_universe(df, self.train_mass_cutoff)

        raw_train = train_universe[self.impression_col].quantile(self.train_q)
        train_threshold = max(raw_train, self.min_train_floor)

        # ---- SCORE / DEFER thresholds (full distribution) ----
        raw_score = df[self.impression_col].quantile(self.score_q)
        raw_defer = df[self.impression_col].quantile(self.defer_q)

        score_threshold = max(raw_score, self.min_score_floor)
        defer_threshold = raw_defer

        return {
            "train_threshold": int(train_threshold),
            "score_threshold": int(score_threshold),
            "defer_threshold": int(defer_threshold),
            "raw_train_p50": int(raw_train),
            "raw_score_p40": int(raw_score),
            "raw_defer_p30": int(raw_defer),
            "train_universe_size": len(train_universe),
            "total_ads": len(df),
        }

    def label_ads(self, df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
        df = df.copy()
        imp = df[self.impression_col]

        df["eligibility"] = np.select(
            [
                imp >= thresholds["train_threshold"],
                imp >= thresholds["score_threshold"],
                imp < thresholds["defer_threshold"],
            ],
            [
                "TRAIN",
                "SCORE",
                "DEFER",
            ],
            default="SCORE",
        )

        return df

def has_non_toll_free_phone(text: str) -> bool:
    """
    Returns True if any detected phone number has a non-toll-free area code.
    (If multiple numbers exist, any non-toll-free will return True.)
    """
    if not text:
        return False
    hits = PHONE_RE.findall(text)
    # findall returns area codes captured by group(1)
    for ac in hits:
        if ac and ac not in TOLL_FREE_AREAS:
            return True
    return False


def category_group(category: str) -> str:
    c = category or ""

    excluded_start_with = [
        "Apparel / Footwear / Accessories",
        "Consumer Products",
        "Cosmetic / Hygiene",
        "Telecommunications",
        "Electronics / Technology",
        "Food / Beverages",
        "Home and Garden",
        "Retail",
        "Automotive / Vehicles - Bicycles / Accessories",
        "Automotive / Vehicles - Car Manufacturers",
        "Automotive / Vehicles - Fuel / Oil / Cleaning",
        "Automotive / Vehicles - Motorcycle and Scooter Manufacturers / Dealers",
        "Automotive / Vehicles - Parts and Accessories",
        "Automotive / Vehicles - Valuation",
        "Education - Universities / Colleges",
        "Government / Organizations - Sporting Organizations",
        "Legal / Financial - Credit / Debit Cards",
        "Entertainment / Media / Leisure - Adult",
        "Entertainment / Media / Leisure - Books",
        "Entertainment / Media / Leisure - Event Ticket Agencies",
        "Entertainment / Media / Leisure - Fishing",
        "Entertainment / Media / Leisure - Mobile Games / Online Games",
        "Entertainment / Media / Leisure - Movie DVD / Blu-ray / Download",
        "Entertainment / Media / Leisure - Movie Release Cinema",
        "Entertainment / Media / Leisure - Pay Per View",
        "Entertainment / Media / Leisure - Race Track",
        "Entertainment / Media / Leisure - Social Media",
        "Entertainment / Media / Leisure - Sports Equipment",
        "Entertainment / Media / Leisure - Sports Events",
        "Entertainment / Media / Leisure - Streaming Subscription / Video on demand",
        "Entertainment / Media / Leisure - Streaming Subscription / Video on demand / Web Series",
        "Entertainment / Media / Leisure - Television Program",
        "Entertainment / Media / Leisure - Video Games",
        "Health - Addiction Products",
        "Health - Allergy / Sinus / Nasal Congestion Products",
        "Health - Anti-Addiction Products",
        "Health - Baby / Infant Care Products",
        "Health - Cold and Flu Products",
        "Health - Diabetes Treatment",
        "Health - Family Planning / Sexual Wellness",
        "Health - First Aid Products",
        "Health - Gym / Fitness Equipment & Accessories",
        "Health - Home Diagnostic Tests",
        "Health - OTC",
        "Health - Pain Relief",
        "Health - Pharmaceutical Companies",
        "Health - Prescription",
        "Health - Sleeping and Snoring Aids",
        "Health - Therapy & Relaxation Products",
        "Health - Vaccinations",
        "Health - Vitamins / Mineral Supplements",
        "Travel - Airlines",
        "Travel - Car Rental & Taxi Services",
        "Travel - Carpooling & Vehicle Sharing",
        "Travel - Cruises / Ferries",
        "Travel - Destinations",
        "Travel - Holiday Parks / Camping",
        "Travel - Hotels / Motels",
        "Travel - Resorts",
        "Travel - Travel Agents / Online Booking / Travel Comparison",
        "Legal / Financial - Banks",
        "Legal / Financial - Insurance Life",
        "Legal / Financial - Insurance Various",
        "Legal / Financial - Insurance Vehicle",
        "Restaurants - Fast Food / Fast Casual",
    ]
    if any(c.startswith(x) for x in excluded_start_with):
        return "excluded"

    # More specific before general
    if c.startswith("Government / Organizations - Public Information Message"):
        return "gov_pim"
    if c.startswith("Government / Organizations - Political Local"):
        return "gov_pol_local"
    if c.startswith("Government / Organizations - Political National"):
        return "gov_pol_nat"
    if c.startswith("Government / Organizations"):
        return "gov_other"

    if c.startswith("Services"):
        return "services"
    if c.startswith("Health"):
        return "health"
    if c.startswith("Restaurants"):
        return "restaurants_table"
    if c.startswith("Automotive / Vehicles - Car Dealer"):
        return "auto_dealer"
    if c.startswith("Automotive / Vehicles"):
        return "auto_other"
    if c.startswith("Entertainment / Media / Leisure"):
        return "entertainment"
    if c.startswith("Travel"):
        return "travel"

    return "other"


@dataclass
class BrandSignals:
    geo_qualified: bool
    geo_source: str
    national_marker: bool
    excluded: bool
    exclusion_reason: str


class BrandLocalNLP:
    """
    One-way override to local.

    Geo signals (in priority order):
      1) non-toll-free phone => local campaign signal
      2) 'of/in/near/serving' followed by (metro/state/capital/region) => geo-qualified
      3) metro/region phrases (REGION_PHRASES)
      4) full state names
      5) capitals (with ambiguity safeguards)

    Entertainment:
      - blocks "network - title" inference,
      - but allows "nbc 5 chicago" (local station pattern + metro/state/capital).
    """

    def __init__(self, region_phrases: Iterable[str]):
        self.region_phrases: Set[str] = {norm_text(x) for x in region_phrases if x}

        esc = sorted((re.escape(p) for p in self.region_phrases), key=len, reverse=True)
        self.region_re = re.compile(r"\b(?:%s)\b" % "|".join(esc)) if esc else None

        # Precompile full-state regex (multi-token states included)
        st_esc = sorted(
            (re.escape(norm_text(s)) for s in US_STATE_FULL), key=len, reverse=True
        )
        self.state_re = re.compile(r"\b(?:%s)\b" % "|".join(st_esc)) if st_esc else None

        # Capitals regex
        cap_esc = sorted(
            (re.escape(norm_text(c)) for c in STATE_CAPITALS), key=len, reverse=True
        )
        self.capital_re = (
            re.compile(r"\b(?:%s)\b" % "|".join(cap_esc)) if cap_esc else None
        )

        # Simple geo-preposition context: "in X", "of X", "near X", "serving X"
        self.prep_re = re.compile(r"\b(of|in|near|serving)\b")

    def _has_national_marker(self, brand_norm: str) -> bool:
        for p in NATIONAL_MARKER_PHRASES:
            if re.search(r"\b" + re.escape(norm_text(p)) + r"\b", brand_norm):
                return True
        return False

    def _entertainment_dash_trap(self, brand: str, cg: str) -> bool:
        if cg != "entertainment" or "-" not in (brand or ""):
            return False
        left = (brand or "").split("-", 1)[0]
        ltoks = set(tokenize(left))
        return bool(ltoks and any(t in NETWORK_TOKENS for t in ltoks))

    def _tail_has_place(self, tail: str) -> Optional[str]:
        """
        Returns a geo_source if tail contains a valid place signal.
        """
        t = norm_text(tail)
        if self.region_re and self.region_re.search(t):
            return "metro_region"
        if self.state_re and self.state_re.search(t):
            return "state_full"
        if self.capital_re and self.capital_re.search(t):
            # capital hit in tail is much less ambiguous because tail is already in geo-prep context
            return "capital"
        return None

    def _has_place_after_preposition(self, brand_norm: str) -> Optional[str]:
        """
        Fixes 'wheel of fortune' errors by requiring a *place* after the preposition.
        """
        for prep in ("of", "in", "near", "serving"):
            needle = f" {prep} "
            if needle not in f" {brand_norm} ":
                continue
            part = brand_norm.split(needle, 1)
            if len(part) < 2:
                continue
            else:
                tail = part[1].strip()
                if not tail:
                    continue
                src = self._tail_has_place(tail)
                if src:
                    return f"prep_{src}"
        return None

    def _capital_safe(self, brand_norm: str, cg: str, cap: str) -> bool:
        """
        Capitals can be very ambiguous. We allow them when:
          - category is gov/education/entertainment/station-ish, OR
          - capital is not in AMBIG_CAPITALS, OR
          - appears with geo-preposition context ("in/of/near/serving <capital>")
        """
        cap = norm_text(cap)
        if cap not in AMBIG_CAPITALS:
            return True

        if cg in {"gov_pim", "gov_other", "gov_pol_local", "gov_pol_nat", "education"}:
            return True

        # preposition context
        if re.search(rf"\b(of|in|near|serving)\s+{re.escape(cap)}\b", brand_norm):
            return True

        return False

    def _auto_model_trap(self, brand_norm: str, cg: str) -> bool:
        """
        Blocks false-local caused by auto model names that are also place names,
        especially when states/capitals are enabled.

        Example blocked: "chevrolet colorado"  (Colorado is a model here, not a location)
        Example allowed:  "chevrolet colorado springs" (metro)
                        "chevrolet colorado of denver" (prep + place)
                        "chevrolet colorado 303-xxx-xxxx" (phone)
                        "chevrolet dealer colorado" (dealer-ish; still requires geo elsewhere)
        """
        if cg not in {"auto_other", "auto_dealer"}:
            return False

        bn = brand_norm
        toks = set(tokenize(bn))

        # Must look like auto creative
        if not (toks & AUTO_BRAND_TOKENS):
            return False

        # Which model-place words are present?
        model_hits = [
            m for m in AUTO_MODEL_PLACE_WORDS if (m in toks or f" {m} " in f" {bn} ")
        ]
        if not model_hits:
            return False

        # Strong geo signals that should override the trap
        if has_non_toll_free_phone(bn):
            return False

        if self._has_non_model_place_after_preposition(bn, model_hits):
            return False

        # If a metro/region phrase exists, allow (e.g., "colorado springs", "san jose")
        if self.region_re and self.region_re.search(bn):
            return False

        # If there is ANY state/capital mention besides the model word itself, allow
        # (e.g., "chevy tahoe texas", "hyundai kona boston")
        # We do this by removing the model hit(s) and re-checking for state/capital.
        scrubbed = bn
        for m in model_hits:
            scrubbed = re.sub(r"\b" + re.escape(m) + r"\b", " ", scrubbed)
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        if self.state_re and self.state_re.search(scrubbed):
            return False
        if self.capital_re and self.capital_re.search(scrubbed):
            return False

        # Dealer tokens alone shouldn't make it geo; they just indicate a local-ish category.
        # If we got here, the ONLY location-looking word is the model-place word => TRAP
        return True

    def _is_generic_oem_auto_creative(self, brand_norm: str) -> bool:
        """
        Detects national OEM-style auto ads that appear under car dealer category
        but lack dealer-specific signals.
        """
        toks = set(tokenize(brand_norm))

        has_auto_brand = bool(toks & AUTO_BRAND_TOKENS)
        has_model_word = bool(
            m in toks or f" {m} " in f" {brand_norm} " for m in AUTO_MODEL_PLACE_WORDS
        )

        has_generic_auto_copy = bool(toks & AUTO_DEALER_GENERIC_WORDS)

        has_dealer_identity = (
            "dealer" in toks
            or "dealership" in toks
            or has_non_toll_free_phone(brand_norm)
        )

        # OEM-style: brand + model + promo language, no dealer identity
        return (
            has_auto_brand
            and has_model_word
            and has_generic_auto_copy
            and not has_dealer_identity
        )

    def _is_generic_oem_auto_creative(self, brand_norm: str) -> bool:
        """
        Detect OEM / national auto creatives that are just
        brand + model (even if model is a city name).
        """
        if not hasattr(self, "_auto_brand_model_re"):
            brands = "|".join(AUTO_BRAND_TOKENS)
            models = "|".join(AUTO_MODEL_PLACE_WORDS)
            self._auto_brand_model_re = re.compile(
                rf"\b({brands})\b\s*[-:|]?\s*\b({models})\b", re.IGNORECASE
            )

        if not self._auto_brand_model_re.search(brand_norm):
            return False

        # Dealer identity overrides OEM assumption
        if (
            "dealer" in brand_norm
            or "dealership" in brand_norm
            or has_non_toll_free_phone(brand_norm)
            or self._has_place_after_preposition(brand_norm)
        ):
            return False

        return True

    def _has_non_model_place_after_preposition(
        self, brand_norm: str, model_hits: List[str]
    ) -> bool:
        """
        True if 'of/in/near/serving' is followed by a place signal
        AFTER removing model-place words (e.g., colorado, tahoe).
        This prevents: 'start the year in a ... colorado' from acting like 'in Colorado'.
        """
        for prep in ("of", "in", "near", "serving"):
            needle = f" {prep} "
            if needle not in f" {brand_norm} ":
                continue

            tail = brand_norm.split(needle, 1)[1].strip()
            if not tail:
                continue

            # Remove model hits from the tail, then check for geo
            scrubbed = tail
            for m in model_hits:
                scrubbed = re.sub(r"\b" + re.escape(m) + r"\b", " ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

            if not scrubbed:
                continue

            if self.region_re and self.region_re.search(scrubbed):
                return True
            if self.state_re and self.state_re.search(scrubbed):
                return True
            if self.capital_re and self.capital_re.search(scrubbed):
                return True

        return False

    def extract_signals(self, brand: str, category: str) -> BrandSignals:
        cg = category_group(category)
        bnorm = norm_text(brand)

        if cg in DISABLE_BRAND_NLP:
            return BrandSignals(
                False, "", False, True, "brand_nlp_disabled_for_category"
            )

        national_marker = self._has_national_marker(bnorm)

        # AUTO MODEL PLACE TRAP
        if self._auto_model_trap(bnorm, cg):
            return BrandSignals(
                False, "", national_marker, True, "auto_model_place_trap"
            )

        # Entertainment dash trap: don't infer geo from RHS title words
        dash_trap = self._entertainment_dash_trap(brand, cg)

        # PHONE RULE (your policy): any non–toll-free phone => local campaign signal
        if has_non_toll_free_phone(brand):
            return BrandSignals(True, "phone_non_toll_free", national_marker, False, "")

        # Preposition + place
        prep_src = self._has_place_after_preposition(bnorm)
        if prep_src:
            return BrandSignals(True, prep_src, national_marker, False, "")

        # If dash trap, ONLY allow geo if it's explicit (metro/state/capital), not inferred from generic title words.
        # Here "explicit" still means a direct match in the full string (e.g., "abc 15 arizona", "nbc 5 chicago")
        if dash_trap:
            # metro/region
            if self.region_re and self.region_re.search(bnorm):
                return BrandSignals(True, "metro_region", national_marker, False, "")
            # state full
            if self.state_re and self.state_re.search(bnorm):
                return BrandSignals(True, "state_full", national_marker, False, "")
            # capital (safe only)
            if self.capital_re:
                m = self.capital_re.search(bnorm)
                if m and self._capital_safe(bnorm, cg, m.group(0)):
                    return BrandSignals(True, "capital", national_marker, False, "")
            return BrandSignals(
                False, "", national_marker, True, "entertainment_dash_title_trap"
            )

        # metro/region phrases
        if self.region_re and self.region_re.search(bnorm):
            return BrandSignals(True, "metro_region", national_marker, False, "")

        # full state names
        if self.state_re and self.state_re.search(bnorm):
            return BrandSignals(True, "state_full", national_marker, False, "")

        # capitals (safe only)
        if self.capital_re:
            m = self.capital_re.search(bnorm)
            if m and self._capital_safe(bnorm, cg, m.group(0)):
                return BrandSignals(True, "capital", national_marker, False, "")

        return BrandSignals(False, "", national_marker, False, "")

    def force_local(self, base_pred: str, brand: str, category: str) -> bool:
        cg = category_group(category)
        sig = self.extract_signals(brand, category)

        if sig.excluded and sig.exclusion_reason == "brand_nlp_disabled_for_category":
            return False

        if cg == "auto_dealer" and self._is_generic_oem_auto_creative(brand):
            return False

        if cg == "auto_dealer" and self._is_generic_oem_auto_creative(brand):
            return False

        # Political Local always local
        if cg == "gov_pol_local":
            return True

        # Public Info: block if explicitly national
        if cg == "gov_pim" and sig.national_marker:
            return False

        # category gating
        # if cg not in ELIGIBLE_FORCE_LOCAL:
        #     return False

        # Entertainment: only allow local station style OR explicit geo (already enforced via signals)
        if cg == "entertainment":
            if LOCAL_STATION_RE.search(norm_text(brand)):
                return bool(sig.geo_qualified and not sig.national_marker)
            return False

        # default
        return bool(sig.geo_qualified and not sig.national_marker)



class LocalNationalClassifier:
    def __init__(
        self,
        nlp: Optional["BrandLocalNLP"] = None,
        impression_col: str = "total_impressions",
        train_mass_cutoff: float = 0.999,
        min_train_floor: int = 100,
        score_quantile=0.333,
    ):
        self.nlp = nlp
        self.impression_col = impression_col
        self.train_mass_cutoff = train_mass_cutoff
        self.min_train_floor = min_train_floor
        self.is_fit = False
        self.score_q = score_quantile

    # -------------------------
    # Feature engineering
    # -------------------------
    def get_localness_score(self, df: pd.DataFrame) -> pd.DataFrame:
        cov = df["coverage_score"]
        mix = df["mix_ratio"]
        ent = df["entropy_norm"]
        reg = df["norm_reg_mix"]

        df.loc[:, "localness_score"] = (
            0.35 * mix + 0.35 * (1 - ent) + 0.20 * (1 - cov) + 0.1 * reg
        )
        return df

    # -------------------------
    # internal TRAIN filter
    # -------------------------
    def _filter_train_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values(self.impression_col, ascending=False)

        total_impr = df[self.impression_col].sum()
        df["cum_mass"] = df[self.impression_col].cumsum() / total_impr

        train_universe = df[df["cum_mass"] <= self.train_mass_cutoff]

        # p50 on TRAIN universe
        train_threshold = max(
            train_universe[self.impression_col].quantile(0.50),
            self.min_train_floor,
        )

        df = df[df[self.impression_col] >= train_threshold].copy()
        df.reset_index(drop=True, inplace=True)
        return df

    # -------------------------
    # Initial thresholds
    # -------------------------
    def initial_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.get_localness_score(df)

        loc = df[df.localness_score >= 0.80].copy()
        nat = df[df.localness_score <= 0.20].copy()

        # Guardrails: if anchors are tiny, quantiles become noisy
        if len(loc) < 200 or len(nat) < 200:
            # fallback: use whole df percentiles (less ideal, but stable)
            loc = df[df.localness_score >= df.localness_score.quantile(0.80)]
            nat = df[df.localness_score <= df.localness_score.quantile(0.20)]

        self.ent_nat_min = nat.entropy_norm.quantile(0.10)
        self.cov_nat_min = nat.coverage_score.quantile(0.10)
        self.mix_nat_max = nat.mix_ratio.quantile(0.90)
        self.reg_nat_max = nat.norm_reg_mix.quantile(0.90)

        self.ent_loc_max = loc.entropy_norm.quantile(0.90)
        self.cov_loc_max = loc.coverage_score.quantile(0.90)
        self.mix_loc_min = loc.mix_ratio.quantile(0.10)
        self.reg_loc_min = loc.norm_reg_mix.quantile(0.10)

        return df

    # -------------------------
    # Base prediction
    # -------------------------
    def base_pred_row(self, row: pd.Series) -> str:
        cov, ent, mix = row["coverage_score"], row["entropy_norm"], row["mix_ratio"]
        d90, r90 = row["dma_in_90"], row["region_count_90"]
        loc, rmx  = row["localness_score"], row["norm_reg_mix"]

        nat_conds = [
            cov >= self.cov_nat_min,
            ent >= self.ent_nat_min,
            mix <= self.mix_nat_max,
            rmx <= self.reg_nat_max,
        ]
        nat_score = sum(nat_conds)

        loc_conds = [
            cov <= self.cov_loc_max,
            ent <= self.ent_loc_max,
            mix >= self.mix_loc_min,
            rmx >= self.reg_loc_min,
        ]
        loc_score = sum(loc_conds)

        if d90 == 1 or r90 == 1:
            return "Local"
        elif loc_score >= 3:
            return "Local"
        elif nat_score >= 3:
            return "National"
        elif loc >= 0.80:
            return "Local"
        elif loc <= 0.20:
            return "National"

        return "Mixed"

    def final_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.initial_thresholds(df)
        df = df.copy()
        df["base_label"] = df.apply(self.base_pred_row, axis=1)

        nat = df[df["base_label"] == "National"].copy()
        loc = df[df["base_label"] == "Local"].copy()

        # Guardrails
        if len(nat) < 200 or len(loc) < 200:
            return df

        self.ent_nat_min = nat.entropy_norm.quantile(0.10)
        self.cov_nat_min = nat.coverage_score.quantile(0.10)
        self.mix_nat_max = nat.mix_ratio.quantile(0.90)
        self.reg_nat_max = nat.norm_reg_mix.quantile(0.90)
        self.loc_nat_max = nat.localness_score.quantile(0.90)
        self.d90_nat_min = nat.dma_in_90.quantile(0.10)

        self.ent_loc_max = loc.entropy_norm.quantile(0.90)
        self.cov_loc_max = loc.coverage_score.quantile(0.90)
        self.mix_loc_min = loc.mix_ratio.quantile(0.10)
        self.reg_loc_min = loc.norm_reg_mix.quantile(0.10)
        self.loc_loc_min = loc.localness_score.quantile(0.10)
        self.d90_loc_max = loc.dma_in_90.quantile(0.90)

        return df
    
    def final_pred_pre_override(self, row: pd.Series) -> str:
        if row["base_label"] == "National":
            return "National"
        elif row["base_label"] == "Local":
            return "Local"

        cov = row["coverage_score"]
        ent = row["entropy_norm"]
        mix = row["mix_ratio"]
        d90 = row["dma_in_90"]
        loc = row["localness_score"]
        sig = row["sig_dma_count"]
        rmx = row["norm_reg_mix"]
        src = row["sig_region_count"]

        nat_conds = [
            cov >= self.cov_nat_min,
            ent >= self.ent_nat_min,
            mix <= self.mix_nat_max,
            loc <= self.loc_nat_max,
            rmx <= self.reg_nat_max,
        ]
        nat_score = sum(nat_conds)

        loc_conds = [
            cov <= self.cov_loc_max,
            ent <= self.ent_loc_max,
            mix >= self.mix_loc_min,
            loc >= self.loc_loc_min,
            rmx >= self.reg_loc_min,
        ]
        loc_score = sum(loc_conds)

        if loc_score >= 4:
            return "Local"
        elif nat_score >= 4:
            return "National"
        elif loc <= self.loc_nat_max:
            return "National"
        elif loc >= self.loc_loc_min:
            return "Local"
        elif loc_score >= 3 and loc_score > nat_score:
            return "Local"
        elif nat_score >= 3 and nat_score > loc_score:
            return "National"
        elif mix <= self.mix_nat_max and d90 >= self.d90_nat_min:
            return "National"
        elif mix >= self.mix_loc_min and d90 <= self.d90_loc_max:
            return "Local"
        elif sig == 0 and mix <= self.mix_nat_max:
            return "National"
        elif cov >= self.cov_nat_min and ent >= self.ent_nat_min:
            return "National"
        elif cov <= self.cov_loc_max and ent <= self.ent_loc_max:
            return "Local"
        elif mix >= self.mix_loc_min and cov <= self.cov_loc_max:
            return "Local"
        elif src >= 6:
            return "National"
        elif loc >= 0.70:
            return "Local"
        elif loc <= 0.40:
            return "National"
    
        return "Mixed"

    def override(self, row: pd.Series) -> str:
        base = str(row["base_final"])
        if self.nlp is None:
            return base

        brand = "" if pd.isna(row.get("brand", "")) else str(row.get("brand", ""))
        cat = "" if pd.isna(row.get("commercial_category", "")) else str(row.get("commercial_category", ""))

        return "Local" if self.nlp.force_local(base, brand, cat) else base
    
    # -------- fit on filtered weekly-impressions dataset --------
    def fit(self, df: pd.DataFrame) -> "LocalNationalClassifier":
        train_df = self._filter_train_df(df)
        if len(train_df) == 0:
            raise ValueError("No ads meet TRAIN eligibility for fitting.")

        self.final_thresholds(train_df)
        self.is_fit = True
        return self

    # -------- predict uses stored thresholds only --------
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.is_fit:
            raise ValueError("Call .fit() before predict().")

        df = self.get_localness_score(df).copy()

        df["base_label"] = df.apply(self.base_pred_row, axis=1)
        df["second_pred"] = df.apply(self.final_pred_pre_override, axis=1)
        df["base_final"] = df["second_pred"].replace({"Mixed": "National"})

        df["final_label"] = (
            df.apply(self.override, axis=1)
            if self.nlp is not None
            else df["base_final"]
        )
        score_threshold = df[self.impression_col].quantile(self.score_q)

        df = df[df[self.impression_col] >= score_threshold].copy()
        df.reset_index(drop=True, inplace=True)

        return df

def final_run(
    df,
    schema: str = 'dev.mohit_gangwani',
    table_part: str = 'ad_labeling_national_local_set',
    date_to_run: str = None):
    nlp = BrandLocalNLP(region_phrases=REGION_PHRASES)
    clf = LocalNationalClassifier(nlp=nlp).fit(df)
    fdf = clf.predict(df)

    fdf.loc[:, 'changed_by_nlp'] = np.where(
        ((fdf.base_final == 'National') & (fdf.final_label == 'Local')),
        'Y', 'N')
    fdf.loc[:, 'brand'] = fdf.brand.astype(str)

    final_df = fdf[
        [
            "ad_id",
            "brand",
            "commercial_category",
            "total_impressions",
            "total_opportunities",
            "coverage_score",
            "entropy_norm",
            "sig_dma_count",
            "dma_in_90",
            "mix_ratio",
            "region_count_90",
            "sig_region_count",
            "region_mix",
            "top2_region_mix",
            "norm_reg_mix",
            "localness_score",
            "changed_by_nlp",
            "final_label",
        ]
    ].copy()

    date_to_run = date_to_run.replace('-', '')
    spark_df = spark.createDataFrame(final_df)
    spark_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{schema}.{table_part}_{date_to_run}")