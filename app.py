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

    (".*(/sports/).*", "ICOM English"),
    (".*(/news/).*", "ICOM English"),
    (".*(/entertainment/).*", "ICOM English"),
    (".*(/viral/).*", "ICOM English"),
    (".*(/business/).*", "ICOM English"),
    (".*(/health/).*", "ICOM English"),
    (".*(/education/).*", "ICOM English"),
    (".*(/lifestyle/).*", "ICOM English"),
    (".*(/smart-buy/).*", "ICOM English"),
    (".*(/video-gallery/).*", "ICOM English"),
    (".*(/travel/).*", "ICOM English"),
    (".*(/topic/sexy/).*", "ICOM English"),
    (".*(/ifsc-code/).*", "ICOM English"),
    (".*(/author/).*", "ICOM English"),
    (".*(/astrology/).*", "ICOM English"),
    (".*(/technology).*", "ICOM English"),
    (".*(/topic/hot-sexy/).*", "ICOM English"),

    (".*(/business-hindi/).*", "ICOM Hindi"),
    (".*(/cricket-hindi/).*", "ICOM Hindi"),
    (".*(/entertainment-hindi/).*", "ICOM Hindi"),
    (".*(/faith-hindi/).*", "ICOM Hindi"),
    (".*(/gallery-hindi/).*", "ICOM Hindi"),
    (".*(/health/).*", "ICOM Hindi"),
    (".*(/india-hindi/).*", "ICOM Hindi"),
    (".*(/lifestyle/).*", "ICOM Hindi"),
    (".*(/news/).*", "ICOM Hindi"),
    (".*(/travel/).*", "ICOM Hindi"),
    (".*(/viral/).*", "ICOM Hindi"),
    (".*(/world-hindi/).*", "ICOM Hindi"),

    (".*(/politics/).*", "Zee Bihar Jharkhand"),
    (".*(/patna/).*", "Zee Bihar Jharkhand"),
    (".*(/video/).*", "Zee Bihar Jharkhand"),
    (".*(/bhojpuri-cinema/).*", "Zee Bihar Jharkhand"),
    (".*(/crime/).*", "Zee Bihar Jharkhand"),
    (".*(/east-champaran/).*", "Zee Bihar Jharkhand"),
    (".*(/west-champaran/).*", "Zee Bihar Jharkhand"),
    (".*(/web-stories/).*", "Zee Bihar Jharkhand"),
    (".*(/live-tv/).*", "Zee Bihar Jharkhand"),
    (".*(/madhepura/).*", "Zee Bihar Jharkhand"),
    (".*(/muzaffarpur/).*", "Zee Bihar Jharkhand"),
    (".*(/gaya/).*", "Zee Bihar Jharkhand"),
    (".*(/begusarai/).*", "Zee Bihar Jharkhand"),
    (".*(/ranchi/).*", "Zee Bihar Jharkhand"),
    (".*(/bihar-assembly-elections-2025).*", "Zee Bihar Jharkhand"),
    (".*(/photo-gallery/).*", "Zee Bihar Jharkhand"),
    (".*(/katihar/).*", "Zee Bihar Jharkhand"),
    (".*(/jehanabad/).*", "Zee Bihar Jharkhand"),
    (".*(/jamui/).*", "Zee Bihar Jharkhand"),
    (".*(/live-updates/).*", "Zee Bihar Jharkhand"),
    (".*(/saran/).*", "Zee Bihar Jharkhand"),
    (".*(/sitamarhi/).*", "Zee Bihar Jharkhand"),
    (".*(/darbhanga/).*", "Zee Bihar Jharkhand"),
    (".*(/rohtas/).*", "Zee Bihar Jharkhand"),
    (".*(/nawada/).*", "Zee Bihar Jharkhand"),
    (".*(/purnia/).*", "Zee Bihar Jharkhand"),
    (".*(/madhubani/).*", "Zee Bihar Jharkhand"),
    (".*(/deoghar/).*", "Zee Bihar Jharkhand"),
    (".*(/lakhisarai/).*", "Zee Bihar Jharkhand"),
    (".*(/kaimur/).*", "Zee Bihar Jharkhand"),
    (".*(/government-jobs/).*", "Zee Bihar Jharkhand"),
    (".*(/bihar/).*", "Zee Bihar Jharkhand"),
    (".*(/bokaro/).*", "Zee Bihar Jharkhand"),
    (".*(/araria/).*", "Zee Bihar Jharkhand"),
    (".*(/giridih/).*", "Zee Bihar Jharkhand"),
    (".*(/bettiah/).*", "Zee Bihar Jharkhand"),
    (".*(/buxar/).*", "Zee Bihar Jharkhand"),
    (".*(/bhojpur/).*", "Zee Bihar Jharkhand"),
    (".*(/banka/).*", "Zee Bihar Jharkhand"),
    (".*(/latehar/).*", "Zee Bihar Jharkhand"),
    (".*(/bhagalpur/).*", "Zee Bihar Jharkhand"),
    (".*(/dhanbad/).*", "Zee Bihar Jharkhand"),
    (".*(/login/).*", "Zee Bihar Jharkhand"),
    (".*(/khagaria/).*", "Zee Bihar Jharkhand"),
    (".*(/kishanganj/).*", "Zee Bihar Jharkhand"),
    (".*(/health/).*", "Zee Bihar Jharkhand"),
    (".*(/hazaribagh/).*", "Zee Bihar Jharkhand"),
    (".*(/chatra/).*", "Zee Bihar Jharkhand"),
    (".*(/jamshedpur/).*", "Zee Bihar Jharkhand"),
    (".*(/gumla/).*", "Zee Bihar Jharkhand"),
    (".*(/gopalganj/).*", "Zee Bihar Jharkhand"),
    (".*(/saharsa/).*", "Zee Bihar Jharkhand"),
    (".*(/siwan/).*", "Zee Bihar Jharkhand"),
    (".*(/religion/).*", "Zee Bihar Jharkhand"),
    (".*(/videos/).*", "Zee Bihar Jharkhand"),
    (".*(/vaishali/).*", "Zee Bihar Jharkhand"),
    (".*(/motihari/).*", "Zee Bihar Jharkhand"),
    (".*(/samastipur/).*", "Zee Bihar Jharkhand"),
    (".*(/khunti/).*", "Zee Bihar Jharkhand"),
    (".*(/munger/).*", "Zee Bihar Jharkhand"),
    (".*(/sheikhpura/).*", "Zee Bihar Jharkhand"),
    (".*(/nalanda/).*", "Zee Bihar Jharkhand"),
    (".*(/supaul/).*", "Zee Bihar Jharkhand"),
    (".*(/lok-sabha-elections/).*", "Zee Bihar Jharkhand"),

    (".*(/hindi/zeeodisha/trending/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/video/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/astrology/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/lifestyle/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/weather/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/photo-gallery).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/entertainment/).*", "Zee Odisha"),
    (".*(/zeeodisha/web-stories/trending/).*", "Zee Odisha"),
    (".*(/zeeodisha/web-stories/lifestyle/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/education-job/).*", "Zee Odisha"),
    (".*(/zeeodisha/web-stories/spirituality/).*", "Zee Odisha"),
    (".*(/zeeodisha/web-stories/entertainment/).*", "Zee Odisha"),
    (".*(/hindi/zeeodisha/sports/).*", "Zee Odisha"),

    (".*(/hindi/india/rajasthan/web-stories/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/photo-gallery-).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/jaipur/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/video/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/live-tv).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/nagaur/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/chittorgargh/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/entertainment/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/jaisalmer/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/ajmer/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/sikar/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/localnews).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/jodhpur/).*", "Zee Rajasthan"),
    (".*(/photos/sports/cricket/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/baran/).*", "Zee Rajasthan"),
    (".*(/hindi/india/rajasthan/live-updates/).*", "Zee Rajasthan"),

    (".*(/hindi/india/up-uttarakhand/lucknow/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/kumbh-mela-).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/photo-gallery).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/meerut/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/interesting-news/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/web-stories).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/gorakhpur/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/kanpur/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/noida/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/up-ki-baat/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/video/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/saharanpur/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/varanasi/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/ghaziabad/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/agra/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/gautambudh-nagar/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/aligarh/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/uttarakhand).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/prayagraj/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/religion/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/lakhimpur-kheri/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/mathura/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/trending-news/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/bareilly/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/moradabad/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/azamgarh/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/dehradun/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/chitrakoot/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/ayodhya/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/utility-news/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/jhansi/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/up-politics/).*", "Zee UP UK"),
    (".*(/hindi/india/up-uttarakhand/mirzapur/).*", "Zee UP UK"),

    (".*(/hindi/zeephh/punjab).*", "Zee PHH"),
    (".*(/hindi/zeephh/religion/).*", "Zee PHH"),
    (".*(/hindi/zeephh/web-stories/).*", "Zee PHH"),
    (".*(/hindi/zeephh/video/).*", "Zee PHH"),
    (".*(/hindi/zeephh/trending-news/).*", "Zee PHH"),
    (".*(/hindi/zeephh/himachal-pradesh/).*", "Zee PHH"),
    (".*(/hindi/zeephh/entertainment/).*", "Zee PHH"),
    (".*(/hindi/zeephh/chandigarh/).*", "Zee PHH"),
    (".*(/hindi/zeephh/sports/).*", "Zee PHH"),
    (".*(/hindi/zeephh/national/).*", "Zee PHH"),
    (".*(/hindi/zeephh/education/).*", "Zee PHH"),
    (".*(/hindi/zeephh/tourism/).*", "Zee PHH"),
    (".*(/hindi/zeephh/politics/).*", "Zee PHH"),
    (".*(/hindi/zeephh/health/).*", "Zee PHH"),
    (".*(/hindi/zeephh/international/).*", "Zee PHH"),

    (".*(/hindi/zee-hindustan/national/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/web-stories/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/world-news/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/video/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/astrology/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/photo-gallery).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/live-tv).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/utility-news/).*", "Zee Hindustan"),
    (".*(/hindi/zee-hindustan/entertainment/).*", "Zee Hindustan"),
    (".*(/web-stories/lifestyle/).*", "Zee Hindustan"),
    (".*(/web-stories/smart-buy/).*", "Zee Hindustan"),

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

    (".*(/hindi/india/delhi-ncr-haryana/photo-gallery-).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/ghaziabad).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/loktantra/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/people).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/shivangi-nagar).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/live-tv).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/cbse).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/pravesh-verma).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/delhi).*", "Delhi NCR Harayana"),
    (".*(/web-stories/).*", "Delhi NCR Harayana"),
    (".*(/video/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/arvind-kejriwal).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/greater-noida).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/videos).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/good-news).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/prime-minister).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/haryana).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/what-).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/who).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/bus).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/noida/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/kuchh-bhi/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/crime).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/health-care-tips/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/faridabad).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/noida).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/live-updates/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/gurugram).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/gda).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/yeida).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/municipal).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/dda).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/bulldozers).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/bulldozer).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/yamuna).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/golden).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/gdma).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/petro).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/expressway).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/path).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/signs).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/daulatabad).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/pradhan).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/vande).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/british).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/registration).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/ration).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/sonipat).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/10-lakh).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/debris).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/kherki).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/work).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/woman).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/hisar).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/minister).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/iit).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/new).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/fire).*", "Delhi NCR Harayana"),
    (".*(/telugu/photo-gallery/).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/hindu).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/dmrc).*", "Delhi NCR Harayana"),
    (".*(/hindi/india/delhi-ncr-haryana/big).*", "Delhi NCR Harayana"),
    (".*(/tags/).*", "Delhi NCR Harayana"),
    (".*(/cricket/).*", "Delhi NCR Harayana"),
    (".*(/photos/sports/).*", "Delhi NCR Harayana"),

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

    (".*(/photos/).*", "CricketCountry Hindi"),
    (".*(/tag/).*", "CricketCountry Hindi"),
    (".*(/news/).*", "CricketCountry Hindi"),
    (".*(/articles/).*", "CricketCountry Hindi"),
    (".*(/videos/).*", "CricketCountry Hindi"),
    (".*(/webstories/).*", "CricketCountry Hindi"),

    (".*(/news/).*", "CricketCountry English"),
    (".*(/live-scores/).*", "CricketCountry English"),
    (".*(/moments-in-history/).*", "CricketCountry English"),
    (".*(/crichin/hindihome/).*", "CricketCountry English"),
    (".*(/series/).*", "CricketCountry English"),
    (".*(/articles/).*", "CricketCountry English"),
    (".*(/photos/).*", "CricketCountry English"),
    (".*(/ipl-2025/).*", "CricketCountry English"),
    (".*(/videos/).*", "CricketCountry English"),
    (".*(/ipl-2025/schedule/).*", "CricketCountry English"),
    (".*(/author/).*", "CricketCountry English"),
    (".*(/criclife/).*", "CricketCountry English"),
    (".*(/ipl/).*", "CricketCountry English"),
    (".*(/login/).*", "CricketCountry English"),
    (".*(/criceng/home/).*", "CricketCountry English"),
    (".*(/Cricket Country Hindi/).*", "CricketCountry English"),
    (".*(/icc-champions-trophy-2025/).*", "CricketCountry English"),
    (".*(/brand-solution/).*", "CricketCountry English"),
    (".*(/cc-magazine/).*", "CricketCountry English"),
    (".*(/privacy-policy/).*", "CricketCountry English"),
    (".*(/about-us/).*", "CricketCountry English"),
    (".*(/partnerships/).*", "CricketCountry English"),
    (".*(/webstories/cricket/t).*", "CricketCountry English"),
    (".*(/results/).*", "CricketCountry English"),
    (".*(/tag/).*", "CricketCountry English"),
    (".*(/cookies-policy/).*", "CricketCountry English"),
    (".*(/short-videos/).*", "CricketCountry English"),
    (".*(/Cricket Country English/Home/).*", "CricketCountry English"),
    (".*(/uncategorized/essential).*", "CricketCountry English"),
    (".*(/players/).*", "CricketCountry English"),
    (".*(/cricket-scores/).*", "CricketCountry English"),
    (".*(/hi/webstories/).*", "CricketCountry English"),
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
