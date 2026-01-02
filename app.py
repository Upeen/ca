import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
from collections import defaultdict
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension,
    Metric, Filter, FilterExpression
)

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(page_title="GA04 Category Report", layout="wide")
st.title("📊 GA4 Monthly Category Report")

# -------------------------------------------------
# PROPERTY ID → SITE NAME
# -------------------------------------------------
view_id_name_mapping = {
    "424738282": "DNA English",
    "424752920": "DNA Hindi",
    "424754635": "ICOM Hindi",
    "424788272": "ICOM English",
    "424733916": "Zee Bengali",
    "424734706": "Zee Odisha",
    "424737620": "Zee PHH",
    "424740324": "Zee Rajasthan",
    "424747120": "Zee Salaam",
    "424748591": "Zee Gujarati",
    "424751136": "Zee Hindustan",
    "424751758": "Zee English",
    "424752916": "Zee Telugu",
    "424755684": "Zee UP UK",
    "424756835": "Zee Hindi",
    "424807672": "Delhi NCR Harayana",
    "424807363": "Zee Bihar Jharkhand",
    "424803703": "Zee MP CG",
    "424803190": "Zee Tamil",
    "424802051": "Zee Malayalam",
    "424797141": "Zee Marathi",
    "424784463": "Zee Kannada",
    "449141964": "WION",
    "424815185": "Zee Biz English",
    "424771953": "Zee Biz Hindi",
    "425235134": "HealthSite English",
    "425228038": "HealthSite Hindi",
    "379536469": "Screenbox",
    "425234014": "Bollywood Life English",
    "425220388": "Bollywood Life Hindi",
    "425245314": "CricketCountry English",
    "425237720": "CricketCountry Hindi",
    "425228771": "Techlusive English",
    "425219576": "Techlusive Hindi",
    "374981587": "MyLord",
    "432213368": "Petuz",
    "429575403": "Travel by India"
}


# -------------------------------------------------
# RAW REGEX → SITE TABLE (ALL PROVIDED ROWS)
# -------------------------------------------------
SITE_REGEX_ROWS = [
    (".*(/lifestyle/).*", "Zee Hindi"),
    (".*(/photos/|/hindi/photos$).*", "Zee Hindi"),
    (".*(/off-beat/).*", "Zee Hindi"),
    (".*(/entertainment/).*", "Zee Hindi"),
    (".*(/sports/).*", "Zee Hindi"),
    (".*(/india/).*", "Zee Hindi"),
    (".*(/career/).*", "Zee Hindi"),
    (".*(/health/).*", "Zee Hindi"),
    (".*(/business/).*", "Zee Hindi"),
    (".*(/religion/).*", "Zee Hindi"),
    (".*(/travel/).*", "Zee Hindi"),
    (".*(/world/).*", "Zee Hindi"),
    (".*(/technology/).*", "Zee Hindi"),
    (".*(/tags/).*", "Zee Hindi"),
    (".*(/science/).*", "Zee Hindi"),
    (".*(/hindi$|/hindi/$|/hindi/live-tv$).*", "Zee Hindi"),
    (".*(/auto-news/).*", "Zee Hindi"),
    (".*(/explainer/).*", "Zee Hindi"),
    (".*(/crime/).*", "Zee Hindi"),

    (".*(/india/).*", "Zee English"),
    (".*(/lifestyle/).*", "Zee English"),
    (".*(/sports/).*", "Zee English"),
    (".*(/cricket/).*", "Zee English"),
    (".*(/entertainment/).*", "Zee English"),
    (".*(/bollywood/).*", "Zee English"),
    (".*(/television/).*", "Zee English"),
    (".*(/regional/).*", "Zee English"),
    (".*(/business/).*", "Zee English"),
    (".*(/personal-finance/).*", "Zee English"),
    (".*(/economy/).*", "Zee English"),
    (".*(/world/).*", "Zee English"),
    (".*(/people/).*", "Zee English"),
    (".*(/auto/).*", "Zee English"),
    (".*(^/$).*", "Zee English"),
    (".*(/live-tv).*", "Zee English"),
    (".*(/mobility/).*", "Zee English"),
    (".*(/education/).*", "Zee English"),
    (".*(/technology/).*", "Zee English"),
    (".*(/health/).*", "Zee English"),
    (".*(/culture/).*", "Zee English"),
    (".*(/tags/).*", "Zee English"),
    (".*(/viral/).*", "Zee English"),

    (".*(/business/).*", "Zee Kannada"),
    (".*(/Entertainment/).*", "Zee Kannada"),
    (".*(/health/).*", "Zee Kannada"),
    (".*(/lifestyle/).*", "Zee Kannada"),
    (".*(/photo-gallery/).*", "Zee Kannada"),
    (".*(/spiritual/).*", "Zee Kannada"),
    (".*(/sports/).*", "Zee Kannada"),
    (".*(/viral/).*", "Zee Kannada"),
    (".*(/world/).*", "Zee Kannada"),

    (".*(/astrology-religion/).*", "Zee Malayalam"),
    (".*(/business/).*", "Zee Malayalam"),
    (".*(/health-lifestyle/).*", "Zee Malayalam"),
    (".*(/India/).*", "Zee Malayalam"),
    (".*(/Kerala/).*", "Zee Malayalam"),
    (".*(/photo-gallery/).*", "Zee Malayalam"),

    (".*(/business-news/).*", "Zee Tamil"),
    (".*(/india/).*", "Zee Tamil"),
    (".*(/lifestyle/).*", "Zee Tamil"),
    (".*(/movies/).*", "Zee Tamil"),
    (".*(/photo-gallery/).*", "Zee Tamil"),
    (".*(/spiritual/).*", "Zee Tamil"),
    (".*(/sports/).*", "Zee Tamil"),
    (".*(/tamil-nadu/).*", "Zee Tamil"),
    (".*(/technology/).*", "Zee Tamil"),
    (".*(/health/).*", "Zee Tamil"),

    (".*(/india/).*", "Zee Telugu"),
    (".*(/lifestyle/).*", "Zee Telugu"),
    (".*(/photo-gallery/).*", "Zee Telugu"),
    (".*(/spiritual/).*", "Zee Telugu"),
    (".*(/technology/).*", "Zee Telugu"),
    (".*(/telangana/).*", "Zee Telugu"),
    (".*(/business/).*", "Zee Telugu"),
    (".*(/entertainment/).*", "Zee Telugu"),
    (".*(/health/).*", "Zee Telugu"),

    (".*(/stock-markets/).*", "Zee Biz Hindi"),
    (".*(/personal-finance/).*", "Zee Biz Hindi"),
    (".*(/real-estate/).*", "Zee Biz Hindi"),
    (".*(/companies/).*", "Zee Biz Hindi"),
    (".*(/banking/).*", "Zee Biz Hindi"),
    (".*(/hindi/live-tv).*", "Zee Biz Hindi"),
    (".*(/technology/).*", "Zee Biz Hindi"),
    (".*(/small-business/).*", "Zee Biz Hindi"),
    (".*(/auto/).*", "Zee Biz Hindi"),
    (".*(/railways/).*", "Zee Biz Hindi"),
    (".*(/economy/).*", "Zee Biz Hindi"),
    (".*(/india/).*", "Zee Biz Hindi"),
    (".*(/travel/).*", "Zee Biz Hindi"),

    (".*(/companies/).*", "Zee Biz English"),
    (".*(/economy-infra/).*", "Zee Biz English"),
    (".*(/india/).*", "Zee Biz English"),
    (".*(/live-tv).*", "Zee Biz English"),
    (".*(/market-news/).*", "Zee Biz English"),
    (".*(/markets/).*", "Zee Biz English"),
    (".*(/personal-finance/).*", "Zee Biz English"),
    (".*(/trending/).*", "Zee Biz English"),
    (".*(/technology/).*", "Zee Biz English"),
    (".*(/budget).*", "Zee Biz English"),
    (".*(/brand-desk/).*", "Zee Biz English"),
    (".*(/agencies/).*", "Zee Biz English"),

    (".*(/photo-gallery/).*", "Zee Gujarati"),
    (".*(/gujarat/).*", "Zee Gujarati"),
    (".*(/business/).*", "Zee Gujarati"),
    (".*(/lifestyle/).*", "Zee Gujarati"),
    (".*(/india/).*", "Zee Gujarati"),
    (".*(/health/).*", "Zee Gujarati"),
    (".*(/spiritual/).*", "Zee Gujarati"),
    (".*(/sports/|/cricket/).*", "Zee Gujarati"),
    (".*(/world/).*", "Zee Gujarati"),
    (".*(/videos/).*", "Zee Gujarati"),
    (".*(/technology/).*", "Zee Gujarati"),
    (".*(/entertainment/).*", "Zee Gujarati"),
    (".*(/gujarat/$|/gujarati$|/live-tv|/gujarati/gujarat$).*", "Zee Gujarati"),
    (".*(/agriculture/).*", "Zee Gujarati"),
    (".*(/relationship/).*", "Zee Gujarati"),
    (".*(/tags/).*", "Zee Gujarati"),
    (".*(/jobs/).*", "Zee Gujarati"),

    (".*(/entertainment/).*", "Zee Marathi"),
    (".*(/health/).*", "Zee Marathi"),
    (".*(/india/).*", "Zee Marathi"),
    (".*(/maharashtra/).*", "Zee Marathi"),
    (".*(/mumbai/).*", "Zee Marathi"),
    (".*(/photos/).*", "Zee Marathi"),
    (".*(/spirituality/).*", "Zee Marathi"),
    (".*(/sports/).*", "Zee Marathi"),
    (".*(/technology/).*", "Zee Marathi"),
    (".*(/western-maharashtra/).*", "Zee Marathi"),
    (".*(/world/).*", "Zee Marathi"),
    (".*(/konkan/).*", "Zee Marathi"),
    (".*(/tags/).*", "Zee Marathi"),

    (".*(/photo-gallery/).*", "HealthSite Hindi"),
    (".*(/diseases-conditions/).*", "HealthSite Hindi"),
    (".*(/diet/).*", "HealthSite Hindi"),
    (".*(/home-remedies/).*", "HealthSite Hindi"),
    (".*(/beauty/).*", "HealthSite Hindi"),
    (".*(/fitness/).*", "HealthSite Hindi"),
    (".*(/baby-names/).*", "HealthSite Hindi"),
    (".*(/parenting/).*", "HealthSite Hindi"),
    (".*(/sexual-health/).*", "HealthSite Hindi"),
    (".*(/pregnancy/).*", "HealthSite Hindi"),

    (".*(/diseases-conditions/).*", "HealthSite English"),
    (".*(/news/).*", "HealthSite English"),
    (".*(/fitness/).*", "HealthSite English"),
    (".*(/baby-names/).*", "HealthSite English"),
    (".*(/beauty/).*", "HealthSite English"),
    (".*(/photo-gallery/).*", "HealthSite English"),
    (".*(/pregnancy/).*", "HealthSite English"),
    (".*(/sexual-health/).*", "HealthSite English"),
    (".*(/ayurveda/).*", "HealthSite English"),
    (".*(/videos/).*", "HealthSite English"),

    (".*(/viral-news/).*", "Zee Salaam"),
    (".*(/web-stories/).*", "Zee Salaam"),
    (".*(/news/).*", "Zee Salaam"),
    (".*(/video/).*", "Zee Salaam"),
    (".*(/hindi/zeesalaam$).*", "Zee Salaam"),
    (".*(/entertainment/).*", "Zee Salaam"),
    (".*(/photo-gallery/).*", "Zee Salaam"),
    (".*(/muslim-world/).*", "Zee Salaam"),
    (".*(/crime-news/).*", "Zee Salaam"),
    (".*(/cricket/).*", "Zee Salaam"),
    (".*(/health/).*", "Zee Salaam"),
    (".*(/israel-hamas-war/).*", "Zee Salaam"),
    (".*(/world-news/).*", "Zee Salaam"),
    (".*(/muslim-news/).*", "Zee Salaam"),

    (".*(/bollywood/).*", "DNA English"),
    (".*(/business/).*", "DNA English"),
    (".*(/cricket/).*", "DNA English"),
    (".*(/education/).*", "DNA English"),
    (".*(/entertainment/).*", "DNA English"),
    (".*(/explainer/).*", "DNA English"),
    (".*(/hollywood/).*", "DNA English"),
    (".*(/india/).*", "DNA English"),
    (".*(/insights/).*", "DNA English"),
    (".*(/lifestyle/).*", "DNA English"),
    (".*(/personal-finance/).*", "DNA English"),
    (".*(/sports/).*", "DNA English"),
    (".*(/technology/).*", "DNA English"),
    (".*(/television/).*", "DNA English"),
    (".*(/viral/).*", "DNA English"),
    (".*(/world/).*", "DNA English"),
    (".*(/health/).*", "DNA English"),

    (".*(/lifestyle/).*", "DNA Hindi"),
    (".*(/spiritual/).*", "DNA Hindi"),
    (".*(/education/).*", "DNA Hindi"),
    (".*(/india/).*", "DNA Hindi"),
    (".*(/science/|/technology/).*", "DNA Hindi"),
    (".*(/health/).*", "DNA Hindi"),
    (".*(/viral/).*", "DNA Hindi"),
    (".*(/entertainment/).*", "DNA Hindi"),
    (".*(/cricket/|/sports/).*", "DNA Hindi"),
    (".*(/hindi/photo-).*", "DNA Hindi"),
    (".*(/business/).*", "DNA Hindi"),
    (".*(/world/).*", "DNA Hindi"),
    (".*(/dna-explainer/).*", "DNA Hindi"),
    (".*(/hind/$|/hindi$).*", "DNA Hindi"),

    (".*(^/$).*", "WION"),
    (".*(/business-economy/).*", "WION"),
    (".*(/entertainment/).*", "WION"),
    (".*(/india/).*", "WION"),
    (".*(/india-news/).*", "WION"),
    (".*(/live-tv).*", "WION"),
    (".*(/opinions/).*", "WION"),
    (".*(/photos/).*", "WION"),
    (".*(/science/).*", "WION"),
    (".*(/sports/).*", "WION"),
    (".*(/technology/).*", "WION"),
    (".*(/trending/).*", "WION"),
    (".*(/videos/).*", "WION"),
    (".*(/world/).*", "WION"),
    (".*(/worldnews/).*", "WION"),
    (".*(/short-videos/).*", "WION"),
    (".*(/life-fun/).*", "WION"),
    (".*(/food-recipe/).*", "WION"),
    (".*(/science-technology/).*", "WION"),

    (".*(^/$).*", "Techlusive English"),
    (".*(/apps/).*", "Techlusive English"),
    (".*(/artificial-intelligence/).*", "Techlusive English"),
    (".*(/automobile/).*", "Techlusive English"),
    (".*(/best-deals/).*", "Techlusive English"),
    (".*(/features/).*", "Techlusive English"),
    (".*(/games/).*", "Techlusive English"),
    (".*(/gaming/).*", "Techlusive English"),
    (".*(/how-to/).*", "Techlusive English"),
    (".*(/laptops/).*", "Techlusive English"),
    (".*(/mobile-phones/).*", "Techlusive English"),
    (".*(/news/).*", "Techlusive English"),
    (".*(/photo-gallery/).*", "Techlusive English"),
    (".*(/reviews/).*", "Techlusive English"),
    (".*(/tag/).*", "Techlusive English"),
    (".*(/telecom/).*", "Techlusive English"),

    (".*(/automobile/).*", "Techlusive Hindi"),
    (".*(/best-deals/).*", "Techlusive Hindi"),
    (".*(/games/).*", "Techlusive Hindi"),
    (".*(/mobile/).*", "Techlusive Hindi"),
    (".*(/news/).*", "Techlusive Hindi"),
    (".*(/photo-gallery/).*", "Techlusive Hindi"),
    (".*(/recharge-plan/).*", "Techlusive Hindi"),
    (".*(/reviews/).*", "Techlusive Hindi"),
    (".*(/tag/).*", "Techlusive Hindi"),
    (".*(/tips-and-tricks/).*", "Techlusive Hindi"),
    (".*(/webstories/).*", "Techlusive Hindi"),
    (".*(/apps/).*", "Techlusive Hindi"),

    (".*(/entertainment/).*", "Zee Bengali"),
    (".*(/health/).*", "Zee Bengali"),
    (".*(/kolkata/).*", "Zee Bengali"),
    (".*(/lifestyle/).*", "Zee Bengali"),
    (".*(/live-tv).*", "Zee Bengali"),
    (".*(/nation/).*", "Zee Bengali"),
    (".*(/photos/).*", "Zee Bengali"),
    (".*(/sports/).*", "Zee Bengali"),
    (".*(/state/).*", "Zee Bengali"),
    (".*(/tags/).*", "Zee Bengali"),
    (".*(/technology/).*", "Zee Bengali"),
    (".*(/videos/).*", "Zee Bengali"),
    (".*(/world/).*", "Zee Bengali"),

    (".*(/sports/).*", "India.com English"),
    (".*(/news/).*", "India.com English"),
    (".*(/entertainment/).*", "India.com English"),
    (".*(/viral/).*", "India.com English"),
    (".*(/business/).*", "India.com English"),
    (".*(/health/).*", "India.com English"),
    (".*(/education/).*", "India.com English"),
    (".*(/lifestyle/).*", "India.com English"),
    (".*(/smart-buy/).*", "India.com English"),
    (".*(/video-gallery/).*", "India.com English"),
    (".*(/travel/).*", "India.com English"),
    (".*(/topic/sexy/).*", "India.com English"),
    (".*(/ifsc-code/).*", "India.com English"),
    (".*(/author/).*", "India.com English"),
    (".*(/astrology/).*", "India.com English"),
    (".*(/technology).*", "India.com English"),
    (".*(/topic/hot-sexy/).*", "India.com English"),

    (".*(/business-hindi/).*", "India.com Hindi"),
    (".*(/cricket-hindi/).*", "India.com Hindi"),
    (".*(/entertainment-hindi/).*", "India.com Hindi"),
    (".*(/faith-hindi/).*", "India.com Hindi"),
    (".*(/gallery-hindi/).*", "India.com Hindi"),
    (".*(/health/).*", "India.com Hindi"),
    (".*(/india-hindi/).*", "India.com Hindi"),
    (".*(/lifestyle/).*", "India.com Hindi"),
    (".*(/news/).*", "India.com Hindi"),
    (".*(/travel/).*", "India.com Hindi"),
    (".*(/viral/).*", "India.com Hindi"),
    (".*(/world-hindi/).*", "India.com Hindi"),

    (".*(/politics/).*", "Zee News Bihar"),
    (".*(/patna/).*", "Zee News Bihar"),
    (".*(/video/).*", "Zee News Bihar"),
    (".*(/bhojpuri-cinema/).*", "Zee News Bihar"),
    (".*(/crime/).*", "Zee News Bihar"),
    (".*(/east-champaran/).*", "Zee News Bihar"),
    (".*(/west-champaran/).*", "Zee News Bihar"),
    (".*(/web-stories/).*", "Zee News Bihar"),
    (".*(/live-tv/).*", "Zee News Bihar"),
    (".*(/madhepura/).*", "Zee News Bihar"),
    (".*(/muzaffarpur/).*", "Zee News Bihar"),
    (".*(/gaya/).*", "Zee News Bihar"),
    (".*(/begusarai/).*", "Zee News Bihar"),
    (".*(/ranchi/).*", "Zee News Bihar"),
    (".*(/bihar-assembly-elections-2025).*", "Zee News Bihar"),
    (".*(/photo-gallery/).*", "Zee News Bihar"),
    (".*(/katihar/).*", "Zee News Bihar"),
    (".*(/jehanabad/).*", "Zee News Bihar"),
    (".*(/jamui/).*", "Zee News Bihar"),
    (".*(/live-updates/).*", "Zee News Bihar"),
    (".*(/saran/).*", "Zee News Bihar"),
    (".*(/sitamarhi/).*", "Zee News Bihar"),
    (".*(/darbhanga/).*", "Zee News Bihar"),
    (".*(/rohtas/).*", "Zee News Bihar"),
    (".*(/nawada/).*", "Zee News Bihar"),
    (".*(/purnia/).*", "Zee News Bihar"),
    (".*(/madhubani/).*", "Zee News Bihar"),
    (".*(/deoghar/).*", "Zee News Bihar"),
    (".*(/lakhisarai/).*", "Zee News Bihar"),
    (".*(/kaimur/).*", "Zee News Bihar"),
    (".*(/government-jobs/).*", "Zee News Bihar"),
    (".*(/bihar/).*", "Zee News Bihar"),
    (".*(/bokaro/).*", "Zee News Bihar"),
    (".*(/araria/).*", "Zee News Bihar"),
    (".*(/giridih/).*", "Zee News Bihar"),
    (".*(/bettiah/).*", "Zee News Bihar"),
    (".*(/buxar/).*", "Zee News Bihar"),
    (".*(/bhojpur/).*", "Zee News Bihar"),
    (".*(/banka/).*", "Zee News Bihar"),
    (".*(/latehar/).*", "Zee News Bihar"),
    (".*(/bhagalpur/).*", "Zee News Bihar"),
    (".*(/dhanbad/).*", "Zee News Bihar"),
    (".*(/login/).*", "Zee News Bihar"),
    (".*(/khagaria/).*", "Zee News Bihar"),
    (".*(/kishanganj/).*", "Zee News Bihar"),
    (".*(/health/).*", "Zee News Bihar"),
    (".*(/hazaribagh/).*", "Zee News Bihar"),
    (".*(/chatra/).*", "Zee News Bihar"),
    (".*(/jamshedpur/).*", "Zee News Bihar"),
    (".*(/gumla/).*", "Zee News Bihar"),
    (".*(/gopalganj/).*", "Zee News Bihar"),
    (".*(/saharsa/).*", "Zee News Bihar"),
    (".*(/siwan/).*", "Zee News Bihar"),
    (".*(/religion/).*", "Zee News Bihar"),
    (".*(/videos/).*", "Zee News Bihar"),
    (".*(/vaishali/).*", "Zee News Bihar"),
    (".*(/motihari/).*", "Zee News Bihar"),
    (".*(/samastipur/).*", "Zee News Bihar"),
    (".*(/khunti/).*", "Zee News Bihar"),
    (".*(/munger/).*", "Zee News Bihar"),
    (".*(/sheikhpura/).*", "Zee News Bihar"),
    (".*(/nalanda/).*", "Zee News Bihar"),
    (".*(/supaul/).*", "Zee News Bihar"),
    (".*(/lok-sabha-elections/).*", "Zee News Bihar"),

    (".*(/hindi/zeeodisha/trending/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/video/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/astrology/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/lifestyle/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/weather/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/photo-gallery).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/entertainment/).*", "Zee News Odisha"),
    (".*(/zeeodisha/web-stories/trending/).*", "Zee News Odisha"),
    (".*(/zeeodisha/web-stories/lifestyle/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/education-job/).*", "Zee News Odisha"),
    (".*(/zeeodisha/web-stories/spirituality/).*", "Zee News Odisha"),
    (".*(/zeeodisha/web-stories/entertainment/).*", "Zee News Odisha"),
    (".*(/hindi/zeeodisha/sports/).*", "Zee News Odisha"),

    (".*(/hindi/india/rajasthan/web-stories/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/photo-gallery-).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/jaipur/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/video/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/live-tv).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/nagaur/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/chittorgargh/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/entertainment/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/jaisalmer/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/ajmer/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/sikar/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/localnews).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/jodhpur/).*", "Zee News Rajasthan"),
    (".*(/photos/sports/cricket/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/baran/).*", "Zee News Rajasthan"),
    (".*(/hindi/india/rajasthan/live-updates/).*", "Zee News Rajasthan"),

    (".*(/hindi/india/up-uttarakhand/lucknow/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/kumbh-mela-).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/photo-gallery).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/meerut/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/interesting-news/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/web-stories).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/gorakhpur/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/kanpur/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/noida/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/up-ki-baat/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/video/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/saharanpur/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/varanasi/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/ghaziabad/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/agra/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/gautambudh-nagar/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/aligarh/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/uttarakhand).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/prayagraj/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/religion/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/lakhimpur-kheri/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/mathura/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/trending-news/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/bareilly/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/moradabad/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/azamgarh/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/dehradun/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/chitrakoot/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/ayodhya/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/utility-news/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/jhansi/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/up-politics/).*", "Zee News UPUK"),
    (".*(/hindi/india/up-uttarakhand/mirzapur/).*", "Zee News UPUK"),

    (".*(/hindi/zeephh/punjab).*", "Zee News PHH"),
    (".*(/hindi/zeephh/religion/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/web-stories/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/video/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/trending-news/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/himachal-pradesh/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/entertainment/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/chandigarh/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/sports/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/national/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/education/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/tourism/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/politics/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/health/).*", "Zee News PHH"),
    (".*(/hindi/zeephh/international/).*", "Zee News PHH"),

    (".*(/hindi/zee-hindustan/national/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/web-stories/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/world-news/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/video/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/astrology/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/photo-gallery).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/live-tv).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/utility-news/).*", "Zee News Hindustan"),
    (".*(/hindi/zee-hindustan/entertainment/).*", "Zee News Hindustan"),
    (".*(/web-stories/lifestyle/).*", "Zee News Hindustan"),
    (".*(/web-stories/smart-buy/).*", "Zee News Hindustan"),

    (".*(/photos/).*", "Bollywood Life English"),
    (".*(/bigg-boss-).*", "Bollywood Life English"),
    (".*(/news-gossip/).*", "Bollywood Life English"),
    (".*(/tv/).*", "Bollywood Life English"),
    (".*(/bigg-boss/).*", "Bollywood Life English"),
    (".*(/bolwod/).*", "Bollywood Life English"),
    (".*(/movies/).*", "Bollywood Life English"),
    (".*(/smart-buy/).*", "Bollywood Life English"),
    (".*(/videos/).*", "Bollywood Life English"),
    (".*(/latest/).*", "Bollywood Life English"),
    (".*(/web-series/).*", "Bollywood Life English"),
    (".*(/bowohi/hindihom/).*", "Bollywood Life English"),
    (".*(/box-office/).*", "Bollywood Life English"),
    (".*(/south-gossip/).*", "Bollywood Life English"),
    (".*(/celeb/).*", "Bollywood Life English"),
    (".*(/reviews/).*", "Bollywood Life English"),
    (".*(/bowohi/webstoriestv/).*", "Bollywood Life English"),
    (".*(/bowohi/webstoriesgossip/).*", "Bollywood Life English"),
    (".*(/cookie-policy/).*", "Bollywood Life English"),
    (".*(/ott/).*", "Bollywood Life English"),
    (".*(/Bollywoodlife Hindi/webstoriesgossip/).*", "Bollywood Life English"),
    (".*(/Bollywoodlife Hindi/webstoriestv/).*", "Bollywood Life English"),
    (".*(/editors-pick/).*", "Bollywood Life English"),
    (".*(/tag/).*", "Bollywood Life English"),
    (".*(/archives/).*", "Bollywood Life English"),
    (".*(/author/).*", "Bollywood Life English"),
    (".*(/tv-shows/).*", "Bollywood Life English"),
    (".*(/tv-show/).*", "Bollywood Life English"),
    (".*(/youtube-2/).*", "Bollywood Life English"),
    (".*(/contact/).*", "Bollywood Life English"),
    (".*(/BollywoodLife English/webstoriestv/).*", "Bollywood Life English"),
    (".*(/hollywood/).*", "Bollywood Life English"),
    (".*(/BollywoodLife English/Gossip/).*", "Bollywood Life English"),
    (".*(/BollywoodLife English/entertainment/).*", "Bollywood Life English"),
    (".*(/video-gallery/).*", "Bollywood Life English"),
    (".*(/Bollywoodlife Hindi/hindihom/).*", "Bollywood Life English"),
    (".*(/BollywoodLife English/webstoriesgossip/).*", "Bollywood Life English"),
    (".*(/interviews/).*", "Bollywood Life English"),
    (".*(/privacy-policy/).*", "Bollywood Life English"),
    (".*(/awards/).*", "Bollywood Life English"),
    (".*(/about_us/).*", "Bollywood Life English"),
    (".*(/viral-stories/).*", "Bollywood Life English"),
    (".*(/Bollywoodlife Hindi/????? ???/).*", "Bollywood Life English"),
    (".*(/cineswami-2/).*", "Bollywood Life English"),
    (".*(/results/).*", "Bollywood Life English"),
    (".*(/world-cinema/).*", "Bollywood Life English"),
    (".*(/brand-solution/).*", "Bollywood Life English"),
    (".*(/news-gossip-buzz/).*", "Bollywood Life English"),
    (".*(/hi/bhojpuri/).*", "Bollywood Life English"),

    (".*(/photos/).*", "Bollywood Life Hindi"),
    (".*(/news-gossip/).*", "Bollywood Life Hindi"),
    (".*(/tv/).*", "Bollywood Life Hindi"),
    (".*(/hi/bigg-boss/).*", "Bollywood Life Hindi"),
    (".*(/videos/).*", "Bollywood Life Hindi"),
    (".*(/hi/tag/).*", "Bollywood Life Hindi"),
    (".*(/hi/bhojpuri/).*", "Bollywood Life Hindi"),
    (".*(/webstories/).*", "Bollywood Life Hindi"),
    (".*(/hi/video-gallery/).*", "Bollywood Life Hindi"),
    (".*(/hi/web-series/).*", "Bollywood Life Hindi"),
    (".*(/hi/box-office/).*", "Bollywood Life Hindi"),
    (".*(/hi/reviews/).*", "Bollywood Life Hindi"),
    (".*(/hi/viral-stories/).*", "Bollywood Life Hindi"),
    (".*(/south-gossip/).*", "Bollywood Life Hindi"),
    (".*(/hi/hollywood/).*", "Bollywood Life Hindi"),

    (".*(/hindi/india/delhi-ncr-haryana/photo-gallery-).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/ghaziabad).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/loktantra/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/people).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/shivangi-nagar).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/live-tv).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/cbse).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/pravesh-verma).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/delhi).*", "DNH"),
    (".*(/web-stories/).*", "DNH"),
    (".*(/video/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/arvind-kejriwal).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/greater-noida).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/videos).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/good-news).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/prime-minister).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/haryana).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/what-).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/who).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/bus).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/noida/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/kuchh-bhi/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/crime).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/health-care-tips/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/faridabad).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/noida).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/live-updates/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/gurugram).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/gda).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/yeida).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/municipal).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/dda).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/bulldozers).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/bulldozer).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/yamuna).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/golden).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/gdma).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/petro).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/expressway).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/path).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/signs).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/daulatabad).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/pradhan).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/vande).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/british).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/registration).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/ration).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/sonipat).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/10-lakh).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/debris).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/kherki).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/work).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/woman).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/hisar).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/minister).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/iit).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/new).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/fire).*", "DNH"),
    (".*(/telugu/photo-gallery/).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/hindu).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/dmrc).*", "DNH"),
    (".*(/hindi/india/delhi-ncr-haryana/big).*", "DNH"),
    (".*(/tags/).*", "DNH"),
    (".*(/cricket/).*", "DNH"),
    (".*(/photos/sports/).*", "DNH"),

    (".*(/web-stories/).*", "Zee MP CG"),
    (".*(/mp/).*", "Zee MP CG"),
    (".*(/chhattisgarh/).*", "Zee MP CG"),
    (".*(/chhindwara/).*", "Zee MP CG"),
    (".*(/mandla/).*", "Zee MP CG"),
    (".*(/shivpuri/).*", "Zee MP CG"),
    (".*(/video/).*", "Zee MP CG"),
    (".*(/singrauli/).*", "Zee MP CG"),
    (".*(/seoni/).*", "Zee MP CG"),
    (".*(/balaghat/).*", "Zee MP CG"),
    (".*(/narsinghpur/).*", "Zee MP CG"),
    (".*(/live-tv).*", "Zee MP CG"),
    (".*(/indore/).*", "Zee MP CG"),
    (".*(/hoshangabad/).*", "Zee MP CG"),
    (".*(/anuppur/).*", "Zee MP CG"),
    (".*(/raisen/).*", "Zee MP CG"),
    (".*(/chhatarpur/).*", "Zee MP CG"),
    (".*(/mp-nama/).*", "Zee MP CG"),
    (".*(/datia/).*", "Zee MP CG"),
    (".*(/ratlam/).*", "Zee MP CG"),
    (".*(/vidisha/).*", "Zee MP CG"),
    (".*(/satna/).*", "Zee MP CG"),
    (".*(/videos/).*", "Zee MP CG"),
    (".*(/bhopal/).*", "Zee MP CG"),
    (".*(/sukma/).*", "Zee MP CG"),
    (".*(/bijapur/).*", "Zee MP CG"),
    (".*(/trending-news/).*", "Zee MP CG"),
    (".*(/tikamgarh/).*", "Zee MP CG"),
    (".*(/neemuch/).*", "Zee MP CG"),
    (".*(/khandwa/).*", "Zee MP CG"),
    (".*(/durg/).*", "Zee MP CG"),
    (".*(/jabalpur/).*", "Zee MP CG"),
    (".*(/localnews).*", "Zee MP CG"),
    (".*(/sidhi/).*", "Zee MP CG"),
    (".*(/ashoknagar/).*", "Zee MP CG"),
    (".*(/shahdol/).*", "Zee MP CG"),
    (".*(/mp-politics/).*", "Zee MP CG"),
    (".*(/bhind/).*", "Zee MP CG"),
    (".*(/gwalior/).*", "Zee MP CG"),
    (".*(/sehore/).*", "Zee MP CG"),
    (".*(/mandsaur/).*", "Zee MP CG"),
    (".*(/jyotish/).*", "Zee MP CG"),
    (".*(/panna/).*", "Zee MP CG"),
    (".*(/surguja/).*", "Zee MP CG"),
    (".*(/health/).*", "Zee MP CG"),
    (".*(/sagar/).*", "Zee MP CG"),
    (".*(/katni/).*", "Zee MP CG"),
    (".*(/technology/).*", "Zee MP CG"),
    (".*(/hindi/india/madhya-pradesh-chhattisgarh/photo-gallery-).*", "Zee MP CG"),
    (".*(/crime-news/).*", "Zee MP CG"),
    (".*(/damoh/).*", "Zee MP CG"),
    (".*(/bemetara/).*", "Zee MP CG"),
    (".*(/ujjain/).*", "Zee MP CG"),
    (".*(/rewa/).*", "Zee MP CG"),
    (".*(/kanker/).*", "Zee MP CG"),
    (".*(/jashpur/).*", "Zee MP CG"),
    (".*(/raipur/).*", "Zee MP CG"),
    (".*(/cricket/).*", "Zee MP CG"),
    (".*(/photo-gallery/).*", "Zee MP CG"),
    (".*(/bilaspur/).*", "Zee MP CG"),

    (".*(/photos/).*", "CC Hindi"),
    (".*(/tag/).*", "CC Hindi"),
    (".*(/news/).*", "CC Hindi"),
    (".*(/articles/).*", "CC Hindi"),
    (".*(/videos/).*", "CC Hindi"),
    (".*(/webstories/).*", "CC Hindi"),

    (".*(/news/).*", "CC English"),
    (".*(/live-scores/).*", "CC English"),
    (".*(/moments-in-history/).*", "CC English"),
    (".*(/crichin/hindihome/).*", "CC English"),
    (".*(/series/).*", "CC English"),
    (".*(/articles/).*", "CC English"),
    (".*(/photos/).*", "CC English"),
    (".*(/ipl-2025/).*", "CC English"),
    (".*(/videos/).*", "CC English"),
    (".*(/ipl-2025/schedule/).*", "CC English"),
    (".*(/author/).*", "CC English"),
    (".*(/criclife/).*", "CC English"),
    (".*(/ipl/).*", "CC English"),
    (".*(/login/).*", "CC English"),
    (".*(/criceng/home/).*", "CC English"),
    (".*(/Cricket Country Hindi/).*", "CC English"),
    (".*(/icc-champions-trophy-2025/).*", "CC English"),
    (".*(/brand-solution/).*", "CC English"),
    (".*(/cc-magazine/).*", "CC English"),
    (".*(/privacy-policy/).*", "CC English"),
    (".*(/about-us/).*", "CC English"),
    (".*(/partnerships/).*", "CC English"),
    (".*(/webstories/cricket/t).*", "CC English"),
    (".*(/results/).*", "CC English"),
    (".*(/tag/).*", "CC English"),
    (".*(/cookies-policy/).*", "CC English"),
    (".*(/short-videos/).*", "CC English"),
    (".*(/Cricket Country English/Home/).*", "CC English"),
    (".*(/uncategorized/essential).*", "CC English"),
    (".*(/players/).*", "CC English"),
    (".*(/cricket-scores/).*", "CC English"),
    (".*(/hi/webstories/).*", "CC English"),
]

# -------------------------------------------------
# BUILD SITE → REGEX MAP (AUTO)
# -------------------------------------------------
def build_site_category_map(rows):
    site_map = defaultdict(list)
    for regex, site in rows:
        site_map[site].append(regex)
    return dict(site_map)

SITE_CATEGORY_MAP = build_site_category_map(SITE_REGEX_ROWS)

# -------------------------------------------------
# SIDEBAR
# -------------------------------------------------
st.sidebar.header("🔐 Authentication")
cred_file = st.sidebar.file_uploader(
    "Upload GA4 Service Account JSON",
    type=["json"]
)

st.sidebar.header("📅 Date Range")
start_date = st.sidebar.date_input("Start Date", datetime(2024, 8, 1))
end_date = st.sidebar.date_input(
    "End Date", datetime.today() - timedelta(days=1)
)

st.sidebar.header("🌐 GA4 Sites")
selected_sites = st.sidebar.multiselect(
    "Select Properties",
    options=list(view_id_name_mapping.keys()),
    format_func=lambda x: f"{view_id_name_mapping[x]} ({x})"
)

# -------------------------------------------------
# DYNAMIC CATEGORY DROPDOWN (SITE-WISE)
# -------------------------------------------------
st.sidebar.header("📂 Categories (Regex)")

available_categories = set()
for pid in selected_sites:
    site = view_id_name_mapping[pid]
    available_categories.update(SITE_CATEGORY_MAP.get(site, []))

selected_categories = st.sidebar.multiselect(
    "Select Category Regex",
    options=sorted(available_categories)
)

st.sidebar.header("🔎 Manual Regex (Optional)")
regex_input = st.sidebar.text_area(
    "One regex per line",
    height=160
)

fetch_btn = st.sidebar.button("🚀 Fetch GA4 Data")

# -------------------------------------------------
# GA4 FETCH FUNCTION
# -------------------------------------------------
def fetch_ga4_data(client, property_id, site, regex, start_date, end_date):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )],
        dimensions=[Dimension(name="year"), Dimension(name="month")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="screenPageViews")
        ],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.FULL_REGEXP,
                    value=regex
                )
            )
        )
    )

    response = client.run_report(request)
    data = []

    for row in response.rows:
        data.append({
            "Site": site,
            "Year_Month": f"{row.dimension_values[0].value}-{row.dimension_values[1].value.zfill(2)}",
            "Users": int(row.metric_values[0].value),
            "Pageviews": int(row.metric_values[1].value),
            "Regex": regex
        })

    return data

# -------------------------------------------------
# MAIN
# -------------------------------------------------
if fetch_btn:
    if not cred_file:
        st.error("❌ Upload GA4 Service Account JSON")
    elif not selected_sites:
        st.error("❌ Select at least one site")
    elif not selected_categories and not regex_input.strip():
        st.error("❌ Select category or add manual regex")
    else:
        with st.spinner("Fetching GA4 data..."):
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(cred_file.getvalue().decode("utf-8")),
                scopes=["https://www.googleapis.com/auth/analytics.readonly"]
            )

            client = BetaAnalyticsDataClient(credentials=credentials)

            manual_regex_list = [
                r.strip() for r in regex_input.splitlines() if r.strip()
            ]

            final_data = []

            for pid in selected_sites:
                site = view_id_name_mapping[pid]
                site_regex = set(SITE_CATEGORY_MAP.get(site, []))

                valid_regex = site_regex.intersection(selected_categories)
                valid_regex.update(manual_regex_list)

                for regex in valid_regex:
                    final_data.extend(
                        fetch_ga4_data(
                            client,
                            pid,
                            site,
                            regex,
                            start_date,
                            end_date
                        )
                    )

            if final_data:
                df = pd.DataFrame(final_data)
                st.success(f"✅ {len(df)} rows fetched")
                st.dataframe(df, use_container_width=True)

                st.download_button(
                    "⬇️ Download CSV",
                    df.to_csv(index=False),
                    "ga4_category_report.csv",
                    "text/csv"
                )
            else:
                st.warning("⚠️ No data returned")
