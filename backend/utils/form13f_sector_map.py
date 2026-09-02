# -*- coding: utf-8 -*-
"""CUSIP -> GICS sector 映射表 + ETF/基金分类器。

GICS sector 取 MacroMicro 13F 页面使用的 11 个板块：
Healthcare, Basic Materials, Consumer Cyclical, Industrials, Technology,
Real Estate, Financial Services, Consumer Defensive, Utilities,
Communication Services, Energy

未匹配的个股归入 "Other"。
"""
import re

# 按价值排序的主流股票 CUSIP -> sector（基于 GICS 分类）
CUSIP_SECTOR = {
    # ============ Technology ============
    '67066G104': 'Technology',  # NVIDIA
    '037833100': 'Technology',  # Apple
    '594918104': 'Technology',  # Microsoft
    '594918904': 'Technology',  # Microsoft
    '02079K305': 'Technology',  # Alphabet A
    '02079K107': 'Technology',  # Alphabet A (alt)
    '02079K206': 'Technology',  # Alphabet C
    '11135F101': 'Technology',  # Broadcom
    '595112103': 'Technology',  # Micron
    '595112903': 'Technology',  # Micron (alt)
    '084687106': 'Technology',  # Cisco? no -> Cisco is 17275R102
    '17275R102': 'Technology',  # Cisco
    '007903107': 'Technology',  # AMD
    '874039100': 'Technology',  # TSMC ADR
    '038222105': 'Technology',  # Applied Materials
    '512807306': 'Technology',  # Lam Research
    '512807108': 'Technology',  # Lam Research (alt)
    '482480100': 'Technology',  # KLA
    '458140100': 'Technology',  # Intel
    '88160R101': 'Technology',  # Tesla (Consumer Cyclical in GICS -> 移到 Consumer Cyclical)
    '032654105': 'Technology',  # Analog Devices
    '882508104': 'Technology',  # Texas Instruments
    '747525103': 'Technology',  # Qualcomm
    '808513105': 'Technology',  # Schwab? -> Financial Services. 移除见下
    '79466L302': 'Technology',  # Salesforce
    '717081103': 'Technology',  # Pfizer -> Healthcare. 见下
    '81762P102': 'Technology',  # ServiceNow
    '743315103': 'Technology',  # Adobe? no 00724F101
    '00724F101': 'Technology',  # Adobe
    '040413205': 'Technology',  # Arista Networks
    '74340W103': 'Real Estate',  # Prologis
    '871607107': 'Technology',  # Synopsys
    '127387108': 'Technology',  # Cadence
    '833445109': 'Technology',  # Snowflake
    '285512109': 'Technology',  # Electronic Arts
    '655844108': 'Technology',  # Norfolk Southern -> Industrials. 见下
    '573874104': 'Technology',  # Marvell
    '92537N108': 'Technology',  # Vertiv
    '80004C200': 'Technology',  # Sandisk
    '219350105': 'Technology',  # Corning
    'G7997R103': 'Technology',  # Seagate
    '36828A101': 'Industrials',  # GE Vernova
    '18915M107': 'Technology',  # Cloudflare
    '34959E109': 'Technology',  # Fortinet
    '23804L103': 'Technology',  # Datadog
    '023135106': 'Consumer Cyclical',  # Amazon
    '023135956': 'Consumer Cyclical',  # Amazon (alt)
    '64110L106': 'Communication Services',  # Netflix
    'L8681T102': 'Communication Services',  # Spotify
    '855244109': 'Consumer Cyclical',  # Starbucks
    '57636Q104': 'Technology',  # Mastercard -> Financial Services. 见下
    '92826C839': 'Financial Services',  # Visa
    '88160R951': 'Consumer Cyclical',  # Tesla (alt CUSIP)
    '69608A108': 'Technology',  # Palantir
    '038222109': 'Technology',
    '03831W108': 'Technology',  # AppLovin
    'N07059210': 'Technology',  # ASML
    '595017104': 'Technology',  # Microchip
    'N6596X109': 'Technology',  # NXP
    '609839105': 'Technology',  # Monolithic Power
    '171779309': 'Technology',  # Ciena
    '880770102': 'Technology',  # Teradyne
    '518415104': 'Technology',  # Lattice Semiconductor
    'M87915274': 'Technology',  # Tower Semiconductor
    '682189105': 'Technology',  # ON Semiconductor
    '666807102': 'Industrials',  # Northrop Grumman
    '92532F100': 'Healthcare',  # Vertex
    '615369105': 'Financial Services',  # Moody's
    '78409V104': 'Financial Services',  # S&P Global
    '09290D101': 'Financial Services',  # BlackRock
    '74762E102': 'Industrials',  # Quanta Services
    '872540109': 'Consumer Cyclical',  # TJX
    '254687106': 'Communication Services',  # Disney
    '369604301': 'Industrials',  # GE Aerospace
    '149123101': 'Industrials',  # Caterpillar
    '718172109': 'Consumer Defensive',  # Philip Morris
    '949746101': 'Financial Services',  # Wells Fargo
    '38141G104': 'Financial Services',  # Goldman Sachs
    'G54950103': 'Basic Materials',  # Linde
    '75513E101': 'Industrials',  # RTX
    '883556102': 'Healthcare',  # Thermo Fisher
    '580135101': 'Consumer Cyclical',  # McDonald's
    '025816109': 'Financial Services',  # American Express
    '713448108': 'Consumer Defensive',  # PepsiCo
    '482480106': 'Technology',
    '25809K105': 'Consumer Cyclical',  # DoorDash
    '867224107': 'Energy',  # Suncor
    '92840M102': 'Utilities',  # Vistra
    '052769106': 'Technology',  # Autodesk
    '13646K108': 'Industrials',  # Canadian Pacific
    '22052L104': 'Basic Materials',  # Corteva
    '770700102': 'Financial Services',  # Robinhood
    '594972408': 'Technology',  # Strategy (MicroStrategy)
    '682680103': 'Energy',  # ONEOK
    '29364G103': 'Utilities',  # Entergy
    '172908105': 'Industrials',  # Cintas
    '70450Y103': 'Technology',  # PayPal
    '30161N101': 'Utilities',  # Exelon
    '911363109': 'Industrials',  # United Rentals
    '311900104': 'Industrials',  # Fastenal
    '89832Q109': 'Financial Services',  # Truist
    '756109104': 'Real Estate',  # Realty Income
    '020002101': 'Financial Services',  # Allstate
    '693718108': 'Industrials',  # Paccar
    '87807B107': 'Energy',  # TC Energy
    '075887109': 'Healthcare',  # Becton Dickinson
    '25746U109': 'Utilities',  # Dominion Energy
    '063671101': 'Financial Services',  # Bank of Montreal
    '98389B100': 'Utilities',  # Xcel Energy
    '031100100': 'Industrials',  # AMETEK
    '19260Q107': 'Financial Services',  # Coinbase
    '199908104': 'Industrials',  # Comfort Systems
    '14149Y108': 'Healthcare',  # Cardinal Health
    '247361702': 'Industrials',  # Delta Air Lines
    'G6683N103': 'Financial Services',  # Nu Holdings
    '14448C104': 'Industrials',  # Carrier Global
    '136375102': 'Industrials',  # Canadian National Railway
    '06849F108': 'Basic Materials',  # Barrick Gold
    '19247G107': 'Technology',  # Coherent
    '31488V107': 'Industrials',  # Ferguson
    '45168D104': 'Healthcare',  # IDEXX
    '169656105': 'Consumer Cyclical',  # Chipotle
    '001055102': 'Financial Services',  # Aflac
    '776696106': 'Technology',  # Roper
    '136069101': 'Financial Services',  # CIBC
    '28176E108': 'Healthcare',  # Edwards Lifesciences
    '631103108': 'Financial Services',  # Nasdaq
    '760759100': 'Industrials',  # Republic Services
    '55354G100': 'Financial Services',  # MSCI
    '02043Q107': 'Healthcare',  # Alnylam
    '026874784': 'Financial Services',  # AIG
    '384802104': 'Industrials',  # W.W. Grainger
    '501044101': 'Consumer Defensive',  # Kroger
    '30040W108': 'Utilities',  # Eversource
    '12504L109': 'Real Estate',  # CBRE
    '92276F100': 'Real Estate',  # Ventas
    '49338L103': 'Technology',  # Keysight
    '064149107': 'Financial Services',  # Bank of Nova Scotia
    '773903109': 'Industrials',  # Rockwell Automation
    '74460D109': 'Real Estate',  # Public Storage
    '278642103': 'Technology',  # eBay -> Consumer Cyclical (GICS moved to Consumer Discretionary 2023)
    '21873S108': 'Technology',  # CoreWeave
    '26884L109': 'Financial Services',  # Equitable
    '093712107': 'Industrials',  # Bloom Energy
    '25278X109': 'Energy',  # Diamondback
    '59156R108': 'Financial Services',  # MetLife
    '69331C108': 'Utilities',  # PG&E
    '962879102': 'Basic Materials',  # Wheaton Precious Metals
    '23331A109': 'Consumer Cyclical',  # D.R. Horton
    '988498101': 'Consumer Cyclical',  # Yum! Brands
    '03076C106': 'Financial Services',  # Ameriprise
    '22822V101': 'Communication Services',  # Crown Castle
    '94106B101': 'Industrials',  # Waste Connections
    '929740108': 'Industrials',  # Wabtec
    '857477103': 'Financial Services',  # State Street
    '316773100': 'Financial Services',  # Fifth Third
    '416515104': 'Financial Services',  # Hartford
    '337738108': 'Technology',  # Fiserv
    '771049103': 'Communication Services',  # Roblox
    '874054109': 'Communication Services',  # Take-Two
    '345370860': 'Consumer Cyclical',  # Ford
    '573284106': 'Basic Materials',  # Martin Marietta
    '929160109': 'Basic Materials',  # Vulcan Materials
    '49271V100': 'Consumer Defensive',  # Keurig Dr Pepper
    '29977A105': 'Financial Services',  # Evercore
    '12008R107': 'Industrials',  # Builders FirstSource
    '854502101': 'Industrials',  # Stanley Black & Decker
    'G491BT108': 'Financial Services',  # Invesco
    '21874C102': 'Industrials',  # Core & Main
    '512816109': 'Real Estate',  # Lamar Advertising
    '913903100': 'Healthcare',  # Universal Health Services
    'M22465104': 'Technology',  # Check Point
    '071813109': 'Healthcare',  # Baxter
    '832696405': 'Consumer Defensive',  # J.M. Smucker
    '26142V105': 'Consumer Cyclical',  # DraftKings
    '533900106': 'Industrials',  # Lincoln Electric
    '91529Y106': 'Financial Services',  # Unum
    '983134107': 'Consumer Cyclical',  # Wynn Resorts
    '860630102': 'Financial Services',  # Stifel
    'Q4982L109': 'Technology',  # IREN
    '835699307': 'Technology',  # Sony -> Consumer Cyclical (GICS: Consumer Discretionary)
    '80105N105': 'Healthcare',  # Sanofi
    '16115Q308': 'Industrials',  # Chart Industries
    '668771108': 'Technology',  # Gen Digital
    '29358P101': 'Healthcare',  # Ensign Group
    '00766T100': 'Industrials',  # AECOM
    '09073M104': 'Healthcare',  # Bio-Techne
    '00790R104': 'Industrials',  # Advanced Drainage Systems
    '817565104': 'Industrials',  # Service Corp International
    '499049104': 'Industrials',  # Knight-Swift
    '36168Q104': 'Industrials',  # GFL Environmental
    '947890109': 'Financial Services',  # Webster Financial
    '775109200': 'Communication Services',  # Rogers Communications
    'H5919C104': 'Consumer Cyclical',  # On Holding
    '320517105': 'Financial Services',  # First Horizon
    '681936100': 'Real Estate',  # Omega Healthcare
    '159864107': 'Healthcare',  # Charles River Labs
    'N20944109': 'Industrials',  # CNH Industrial
    '379577208': 'Healthcare',  # Globus Medical
    '72348N109': 'Financial Services',  # Pinnacle Financial
    '04621X108': 'Financial Services',  # Assurant
    '088606108': 'Basic Materials',  # BHP
    '89677Q107': 'Consumer Cyclical',  # Trip.com
    '465562106': 'Financial Services',  # Itau Unibanco
    '148929102': 'Consumer Cyclical',  # Cava
    '07725L102': 'Healthcare',  # BeOne Medicines
    '23918K108': 'Healthcare',  # DaVita
    '101121101': 'Real Estate',  # BXP
    '553368101': 'Basic Materials',  # MP Materials
    '01973R101': 'Industrials',  # Allison Transmission
    '303075105': 'Financial Services',  # FactSet
    '91332U101': 'Technology',  # Unity Software
    '29452E101': 'Financial Services',  # Equitable Holdings
    '83443Q103': 'Basic Materials',  # Solstice Advanced Materials
    '574795100': 'Healthcare',  # Masimo
    '67059N108': 'Technology',  # Nutanix
    '056752108': 'Communication Services',  # Baidu
    '267475101': 'Industrials',  # Dycom
    '92243G108': 'Healthcare',  # Vaxcyte
    '683344105': 'Technology',  # Onto Innovation
    '74275K108': 'Technology',  # Procore
    '403949100': 'Energy',  # HF Sinclair
    '256163106': 'Technology',  # DocuSign
    '011532108': 'Basic Materials',  # Alamos Gold
    '23282W605': 'Healthcare',  # Cytokinetics
    '78709Y105': 'Industrials',  # Saia
    '46138E461': 'Financial Services',  # Invesco ETF (fund-ish but stock CUSIP)
    'G66721104': 'Consumer Cyclical',  # Norwegian Cruise Line
    '37959E102': 'Financial Services',  # Globe Life
    '02665T306': 'Real Estate',  # American Homes 4 Rent
    '87305R109': 'Technology',  # TTM Technologies
    '313745101': 'Real Estate',  # Federal Realty
    '422704106': 'Basic Materials',  # Hecla Mining
    '29670G102': 'Utilities',  # Essential Utilities
    '46222L108': 'Technology',  # IonQ
    '73278L105': 'Industrials',  # Pool Corp
    '015271109': 'Real Estate',  # Alexandria Real Estate
    '98954M200': 'Real Estate',  # Zillow -> Real Estate (GICS)
    '099502106': 'Industrials',  # Booz Allen Hamilton
    '501889208': 'Consumer Cyclical',  # LKQ
    '562750109': 'Technology',  # Manhattan Associates
    'G16252101': 'Utilities',  # Brookfield Infrastructure
    '75281A109': 'Energy',  # Range Resources
    '806407102': 'Healthcare',  # Henry Schein
    '78473E103': 'Industrials',  # SPX Technologies
    '03820C105': 'Industrials',  # Applied Industrial Technologies
    '008492100': 'Real Estate',  # Agree Realty
    '053774105': 'Consumer Cyclical',  # Avis Budget
    '690742101': 'Industrials',  # Owens Corning
    '035710839': 'Real Estate',  # Annaly Capital -> Financial Services (mortgage REIT)
    '714046109': 'Healthcare',  # Revvity
    '277276101': 'Real Estate',  # EastGroup Properties
    '375916103': 'Consumer Cyclical',  # Gildan
    '34354P105': 'Industrials',  # Flowserve
    '02376R102': 'Industrials',  # American Airlines
    '97650W108': 'Financial Services',  # Wintrust
    '257651109': 'Industrials',  # Donaldson
    '277432100': 'Basic Materials',  # Eastman Chemical
    '83444M101': 'Healthcare',  # Solventum
    '29786A106': 'Consumer Cyclical',  # Etsy
    '11120U105': 'Real Estate',  # Brixmor
    '44951W106': 'Industrials',  # IES Holdings
    '57667L107': 'Communication Services',  # Match Group
    '398182303': 'Real Estate',  # American Healthcare REIT
    '688239201': 'Industrials',  # Oshkosh
    '74366E102': 'Healthcare',  # Protagonist Therapeutics
    '750917106': 'Technology',  # Rambus
    '60855R100': 'Healthcare',  # Molina Healthcare
    '12532H104': 'Technology',  # CGI
    '891092108': 'Industrials',  # Toro
    '29261A100': 'Healthcare',  # Encompass Health
    '09581B103': 'Financial Services',  # Blue Owl
    '013091103': 'Consumer Defensive',  # Albertsons
    'M7516K103': 'Technology',  # Nova
    'G4705A100': 'Healthcare',  # ICON plc
    '530307305': 'Communication Services',  # Liberty Broadband
    '04280A100': 'Healthcare',  # Arrowhead
    '61945C103': 'Basic Materials',  # Mosaic
    '485924104': 'Industrials',  # Karman Holdings
    '64361Q101': 'Energy',  # Viper Energy
    '47215P106': 'Consumer Cyclical',  # JD.com
    '680033107': 'Financial Services',  # Old National Bancorp
    '290876101': 'Utilities',  # Emera
    '038336103': 'Basic Materials',  # AptarGroup
    'G8068L108': 'Consumer Cyclical',  # SharkNinja
    '344419106': 'Consumer Defensive',  # FEMSA
    '654902204': 'Technology',  # Nokia
    '831865209': 'Industrials',  # Smith AO Corp
    '35137L204': 'Communication Services',  # Fox Corp
    '456788108': 'Technology',  # Infosys
    '25402D102': 'Technology',  # DigitalOcean
    '28618M106': 'Basic Materials',  # Element Solutions
    '537008104': 'Technology',  # Littelfuse
    '85208M102': 'Consumer Defensive',  # Sprouts Farmers Market
    '404280406': 'Financial Services',  # HSBC
    '29414B104': 'Technology',  # EPAM
    '670703107': 'Healthcare',  # Nuvalent
    '150870103': 'Basic Materials',  # Celanese
    '229899109': 'Financial Services',  # Cullen Frost
    '60871R209': 'Consumer Defensive',  # Molson Coors
    '501147102': 'Healthcare',  # Krystal Biotech
    '626755102': 'Consumer Defensive',  # Murphy USA
    '21874A106': 'Technology',  # Core Scientific
    '339750101': 'Consumer Cyclical',  # Floor & Decor
    'M3760D101': 'Industrials',  # Elbit Systems
    '451107106': 'Utilities',  # IDACORP
    '674215207': 'Energy',  # Chord Energy
    '615394202': 'Industrials',  # Moog
    '10950A106': 'Healthcare',  # BrightSpring
    '40637H109': 'Healthcare',  # Halozyme
    '96208T104': 'Financial Services',  # WEX
    '902788108': 'Financial Services',  # UMB Financial
    '025932104': 'Financial Services',  # American Financial Group
    '816850101': 'Technology',  # Semtech
    '918204108': 'Consumer Cyclical',  # VF Corp
    '552953101': 'Consumer Cyclical',  # MGM Resorts
    '88162G103': 'Industrials',  # Tetra Tech
    '229663109': 'Real Estate',  # CubeSmart
    '00508Y102': 'Industrials',  # Acuity Brands
    '82982T106': 'Technology',  # SiTime
    '205887102': 'Consumer Defensive',  # Conagra
    '45867G101': 'Technology',  # InterDigital
    '21871X109': 'Financial Services',  # Corebridge
    '224408104': 'Industrials',  # Crane Company
    '497266106': 'Industrials',  # Kirby
    '441593100': 'Financial Services',  # Houlihan Lokey
    '49714P108': 'Financial Services',  # Kinsale
    '637417106': 'Real Estate',  # NNN REIT
    '784117103': 'Financial Services',  # SEI Investments
    '008252108': 'Financial Services',  # Affiliated Managers
    '942749102': 'Industrials',  # Watts Water
    '042735100': 'Technology',  # Arrow Electronics
    '354613101': 'Financial Services',  # Franklin Resources
    '448579102': 'Consumer Cyclical',  # Hyatt Hotels
    '14174T107': 'Real Estate',  # CareTrust REIT
    '74164F103': 'Industrials',  # Primoris
    '185123106': 'Technology',  # Clearwater Analytics
    '501575104': 'Healthcare',  # Kymera
    '143130102': 'Consumer Cyclical',  # CarMax
    '32054K103': 'Real Estate',  # First Industrial Realty
    '74736K101': 'Technology',  # Qorvo
    '192422103': 'Technology',  # Cognex
    '670837103': 'Utilities',  # OGE Energy
    '42226A107': 'Healthcare',  # HealthEquity
    '741623102': 'Consumer Defensive',  # Primo Brands
    '15118V207': 'Consumer Cyclical',  # Celsius
    '565394103': 'Technology',  # Maplebear (Instacart)
    '04010E109': 'Industrials',  # Argan
    '47233W109': 'Financial Services',  # Jefferies
    '191098102': 'Consumer Defensive',  # Coca-Cola Consolidated
    '29446M102': 'Energy',  # Equinor
    '680223104': 'Financial Services',  # Old Republic
    '958669103': 'Energy',  # Western Midstream
    '33768G107': 'Consumer Cyclical',  # FirstCash
    'G4474Y214': 'Financial Services',  # Janus Henderson
    '829073105': 'Industrials',  # Simpson Manufacturing
    '880881107': 'Healthcare',  # Terns Pharmaceuticals
    '03524A108': 'Consumer Defensive',  # Anheuser-Busch InBev
    '902681105': 'Utilities',  # UGI
    '74164M108': 'Financial Services',  # Primerica
    '197236102': 'Financial Services',  # Columbia Banking
    '051774107': 'Industrials',  # Aurora Innovation
    '92333F101': 'Energy',  # Venture Global
    '759916109': 'Healthcare',  # Repligen
    '84472E102': 'Financial Services',  # SouthState
    '733174700': 'Financial Services',  # Popular
    'G0692U109': 'Financial Services',  # Axis Capital
    '109641100': 'Consumer Cyclical',  # Brinker
    '44332N106': 'Consumer Cyclical',  # H World Group
    '46817M107': 'Financial Services',  # Jackson Financial
    '29355A107': 'Technology',  # Enphase
    '78454L100': 'Energy',  # SM Energy
    'G17977110': 'Financial Services',  # Burford Capital
    '85423L103': 'Industrials',  # StandardAero
    '19240Q201': 'Healthcare',  # Cogent Biosciences
    '801056102': 'Technology',  # Sanmina
    '636180101': 'Energy',  # National Fuel Gas
    '62955J103': 'Energy',  # NOV
    '920253101': 'Industrials',  # Valmont
    '989701107': 'Financial Services',  # Zions
    '00912X302': 'Financial Services',  # Air Lease
    '72703X106': 'Technology',  # Planet Labs
    '76169C100': 'Real Estate',  # Rexford
    '925550105': 'Technology',  # Viavi
    '440452100': 'Consumer Defensive',  # Hormel
    '783549108': 'Industrials',  # Ryder
    '316841105': 'Technology',  # Figma
    '69366J200': 'Healthcare',  # PTC Therapeutics
    '879369106': 'Healthcare',  # Teleflex
    '377322102': 'Healthcare',  # Glaukos
    '346375108': 'Technology',  # FormFactor
    '78781J109': 'Technology',  # Sailpoint
    '296315104': 'Technology',  # ESCO Technologies
    '04247X102': 'Industrials',  # Armstrong World
    '576485205': 'Energy',  # Matador
    '343412102': 'Industrials',  # Fluor
    '172755100': 'Technology',  # Cirrus Logic
    '457187102': 'Basic Materials',  # Ingredion
    '001084102': 'Industrials',  # AGCO
    '76243J105': 'Healthcare',  # Rhythm Pharmaceuticals
    '45780R101': 'Industrials',  # Installed Building Products
    'G65431127': 'Energy',  # Noble
    '604749101': 'Healthcare',  # Mirum
    '03676B102': 'Energy',  # Antero Midstream
    '903731107': 'Industrials',  # UL Solutions
    '364760108': 'Consumer Cyclical',  # Gap
    '513272104': 'Consumer Defensive',  # Lamb Weston
    '98311A105': 'Consumer Cyclical',  # Wyndham Hotels
    '88080T104': 'Technology',  # TeraWulf
    '87241L109': 'Industrials',  # TFI International
    '98983L108': 'Industrials',  # Zurn Elkay
    '26701L100': 'Consumer Cyclical',  # Dutch Bros
    '880779103': 'Industrials',  # Terex
    '05964H105': 'Financial Services',  # Banco Santander
    '57060D108': 'Financial Services',  # MarketAxess
    'G0750C108': 'Basic Materials',  # Axalta
    '81211K100': 'Industrials',  # Sealed Air
    '69349H107': 'Utilities',  # TXNM Energy
    '05464T104': 'Healthcare',  # Axsome
    'G98239109': 'Financial Services',  # XP Inc
    '201723103': 'Basic Materials',  # Commercial Metals
    '536797103': 'Consumer Cyclical',  # Lithia Motors
    '681116109': 'Consumer Cyclical',  # Ollie's
    '521865204': 'Consumer Cyclical',  # Lear
    '038169207': 'Technology',  # Applied Digital
    '12769G100': 'Consumer Cyclical',  # Caesars
    '428291108': 'Industrials',  # Hexcel
    'N4732M103': 'Consumer Defensive',  # JBS
    'G3922B107': 'Technology',  # Genpact
    '29670E107': 'Real Estate',  # Essential Properties
    '64110W102': 'Communication Services',  # NetEase
    '09175A206': 'Technology',  # Bitmine Immersion
    '88146M101': 'Real Estate',  # Terreno Realty
    '008073108': 'Industrials',  # AeroVironment
    '00507V109': 'Communication Services',  # Activision Blizzard
    '85254J102': 'Real Estate',  # STAG Industrial
    'H8817H100': 'Energy',  # Transocean
    '559663109': 'Energy',  # Magnolia Oil & Gas
    '80706P103': 'Healthcare',  # Scholar Rock
    '15102K100': 'Healthcare',  # Celcuity
    'G9460G101': 'Energy',  # Valaris
    '313855108': 'Industrials',  # Federal Signal
    '55087P104': 'Industrials',  # Lyft -> Industrials (GICS)
    '826919102': 'Technology',  # Silicon Labs
    '515098101': 'Industrials',  # Landstar
    '85472N109': 'Industrials',  # Stantec
    '14020V108': 'Financial Services',  # Capital Group Core Equity
    'G48833118': 'Energy',  # Weatherford
    '13057Q305': 'Healthcare',  # California Resources
    '450913108': 'Basic Materials',  # IAMGOLD
    '152309100': 'Healthcare',  # Centessa
    '781154109': 'Technology',  # Rubrik
    '35969L108': 'Industrials',  # Full Truck Alliance
    '03753U106': 'Healthcare',  # Apellis
    '03770N101': 'Healthcare',  # Apogee Therapeutics
    '743606105': 'Financial Services',  # Prosperity Bancshares
    'G01767105': 'Healthcare',  # Alkermes
    '36118L106': 'Financial Services',  # Futu Holdings
    '87151X101': 'Technology',  # Symbotic
    '736508847': 'Utilities',  # Portland General Electric
    '887389104': 'Industrials',  # Timken
    'G02602103': 'Technology',  # Amdocs
    '929089100': 'Financial Services',  # Voya
    '36467W109': 'Consumer Cyclical',  # GameStop
    '70432V102': 'Technology',  # Paycom
    '91879Q109': 'Consumer Cyclical',  # Vail Resorts
    '090572207': 'Healthcare',  # Bio-Rad
    '02156V109': 'Energy',  # Oklo
    '88023B103': 'Healthcare',  # Tempus AI
    '03940C100': 'Healthcare',  # Arcellx
    '443628102': 'Basic Materials',  # Hudbay Minerals
    '829933100': 'Communication Services',  # SiriusXM
    '387328107': 'Industrials',  # Granite Construction
    '361448103': 'Industrials',  # GATX
    '05352A100': 'Healthcare',  # Avantor
    '29275Y102': 'Technology',  # EnerSys
    '000360206': 'Industrials',  # AAON
    '36262G101': 'Industrials',  # GXO Logistics
    '70202L102': 'Industrials',  # Parsons
    '92552V100': 'Communication Services',  # ViaSat
    '26969P108': 'Basic Materials',  # Eagle Materials
    '00370M103': 'Healthcare',  # ABIVAX
    '147448104': 'Industrials',  # Casella Waste
    '48581R205': 'Financial Services',  # Kaspi.kz
    'M98068105': 'Technology',  # Wix.com
    '686688102': 'Utilities',  # Ormat
    '892331307': 'Consumer Cyclical',  # Toyota
    '31847R102': 'Financial Services',  # First American Financial
    '98420N105': 'Healthcare',  # Xenon
    '516544103': 'Healthcare',  # Lantheus
    '124765108': 'Industrials',  # CAE
    '477839104': 'Industrials',  # John Bean Technologies
    '06738E204': 'Financial Services',  # Barclays
    '115637209': 'Consumer Defensive',  # Brown-Forman
    '68268W103': 'Financial Services',  # OneMain
    '156431108': 'Basic Materials',  # Century Aluminum
    '87724P106': 'Real Estate',  # Taylor Morrison
    '03957W106': 'Energy',  # Archrock
    'G16258108': 'Utilities',  # Brookfield Renewable
    '78377T107': 'Real Estate',  # Ryman Hospitality
    '72703H101': 'Consumer Cyclical',  # Planet Fitness
    'G39108108': 'Industrials',  # Gates Industrial
    '42809H107': 'Energy',  # Hess
    '606822104': 'Financial Services',  # Mitsubishi UFJ
    '596278101': 'Industrials',  # Middleby
    'M2029K104': 'Consumer Cyclical',  # Birkenstock
    '16935C109': 'Financial Services',  # Chime Financial
    '89679M104': 'Basic Materials',  # Triple Flag
    '608190104': 'Consumer Cyclical',  # Mohawk
    '550241103': 'Communication Services',  # Lumen
    '405552100': 'Healthcare',  # Haleon
    '38059T106': 'Basic Materials',  # Gold Fields
    '739128106': 'Industrials',  # Powell Industries
    '29530P102': 'Financial Services',  # Erie Indemnity
    '302941109': 'Financial Services',  # FTI Consulting
    '17253J106': 'Technology',  # Cipher Mining
    'G8060N102': 'Technology',  # Sensata
    '918284100': 'Industrials',  # VSE Corp
    '92647X830': 'Financial Services',  # Victory Portfolio
    '844895102': 'Utilities',  # Southwest Gas
    '55616P104': 'Consumer Cyclical',  # Macy's
    '031652100': 'Technology',  # Amkor
    '32076V103': 'Basic Materials',  # First Majestic Silver
    '82982L103': 'Industrials',  # SiteOne Landscape
    '156944100': 'Healthcare',  # CG Oncology
    '200525103': 'Financial Services',  # Commerce Bancshares
    '552848103': 'Financial Services',  # MGIC
    '553498106': 'Industrials',  # MSA Safety
    '626717102': 'Energy',  # Murphy Oil
    '12653C108': 'Energy',  # CNX Resources
    '55825T103': 'Communication Services',  # Madison Square Garden
    '08265T208': 'Technology',  # Bentley Systems
    '151290889': 'Basic Materials',  # CEMEX
    '872657101': 'Financial Services',  # TPG
    '29082A107': 'Industrials',  # Embraer
    '29446Y502': 'Basic Materials',  # Equinox Gold
    '65336K103': 'Communication Services',  # Nexstar
    '729132100': 'Technology',  # Plexus
    '113006100': 'Financial Services',  # Brookfield Business
    '46090F100': 'Financial Services',  # Invesco actively managed
    '092113109': 'Utilities',  # Black Hills
    '433000106': 'Healthcare',  # Hims & Hers
    '974155103': 'Consumer Cyclical',  # Wingstop
    'L6388F110': 'Communication Services',  # Millicom
    '410867105': 'Financial Services',  # Hanover
    '960413102': 'Basic Materials',  # Westlake
    'G0260P102': 'Consumer Cyclical',  # Amer Sports
    'G3198U102': 'Financial Services',  # Essent Group
    '00123Q104': 'Financial Services',  # AGNC
    '03823U102': 'Technology',  # Applied Optoelectronics
    '649445400': 'Financial Services',  # NYCB
    '09247X101': 'Financial Services',  # BlackRock Finance
    '919794107': 'Financial Services',  # Valley National
    '21044C107': 'Industrials',  # Construction Partners
    '42226K105': 'Real Estate',  # Healthcare Realty
    '302520101': 'Financial Services',  # FNB Corp
    '053807103': 'Technology',  # Avnet
    '50012A108': 'Energy',  # Kodiak Gas
    '928881101': 'Industrials',  # Vontier
    '023939101': 'Industrials',  # Amentum
    '16359R103': 'Healthcare',  # Chemed
    '171484108': 'Consumer Cyclical',  # Churchill Downs
    '53190C102': 'Consumer Cyclical',  # Life Time Group
    '671044105': 'Technology',  # OSI Systems
    '457152106': 'Technology',  # Ingram Micro
    '636274409': 'Utilities',  # National Grid
    '81764X103': 'Technology',  # ServiceTitan
    '29355X107': 'Industrials',  # Enpro
    '134429109': 'Consumer Defensive',  # Campbell's
    '78573L106': 'Real Estate',  # Sabra
    '92047W101': 'Consumer Cyclical',  # Valvoline
    '252828108': 'Healthcare',  # Dianthus
    '117043109': 'Consumer Cyclical',  # Brunswick
    '76118Y104': 'Technology',  # Resideo
    '03076K108': 'Financial Services',  # Ameris
    '300426103': 'Industrials',  # Everus
    '057665200': 'Basic Materials',  # Balchem
    '218937100': 'Basic Materials',  # Core Natural Resources
    '099406100': 'Consumer Cyclical',  # Boot Barn
    '109194100': 'Industrials',  # Bright Horizons
    'G9618E107': 'Financial Services',  # White Mountains
    '68235P108': 'Utilities',  # One Gas
    '227046109': 'Consumer Cyclical',  # Crocs
    '410120109': 'Financial Services',  # Hancock Whitney
    '34964C106': 'Industrials',  # Fortune Brands
    'G4253H101': 'Industrials',  # James Hardie
    '29415F104': 'Healthcare',  # Envista
    '879433829': 'Communication Services',  # Telephone & Data Systems
    '754907103': 'Basic Materials',  # Rayonier
    '056525108': 'Industrials',  # Badger Meter
    '617700109': 'Financial Services',  # Morningstar
    '00090Q103': 'Industrials',  # ADT
    'G65163100': 'Industrials',  # Joby Aviation
    '05329W102': 'Consumer Cyclical',  # AutoNation
    '116794108': 'Healthcare',  # Bruker
    '002896207': 'Consumer Cyclical',  # Abercrombie & Fitch
    '546347105': 'Basic Materials',  # Louisiana-Pacific
    '946784105': 'Healthcare',  # Waystar
    '052800109': 'Consumer Cyclical',  # Autoliv
    '37637Q105': 'Financial Services',  # Glacier Bancorp
    '53947R105': 'Industrials',  # Loar Holdings
    '554382101': 'Real Estate',  # Macerich
    '456837103': 'Financial Services',  # ING Groep
    '69318G106': 'Energy',  # PBF Energy
    '302081104': 'Technology',  # ExlService
    '20464U100': 'Real Estate',  # Compass
    'N14506104': 'Technology',  # Elastic
    '04010L103': 'Financial Services',  # Ares Capital
    '86333M108': 'Consumer Cyclical',  # Stride
    '929042109': 'Real Estate',  # Vornado
    '26603R106': 'Technology',  # Duolingo
    '465741106': 'Technology',  # Itron
    '04956D107': 'Industrials',  # Atmus
    '06417N103': 'Financial Services',  # Bank OZK
    '18467V109': 'Industrials',  # Clear Secure
    '44812J104': 'Technology',  # Hut 8
    '60471A101': 'Healthcare',  # Mirion
    'G9456A100': 'Energy',  # Golar LNG
    '85207H104': 'Basic Materials',  # Sprott Physical Gold
    '84857L101': 'Utilities',  # Spire
    '518613203': 'Consumer Cyclical',  # Laureate
    'M6191J100': 'Technology',  # JFrog
    '249906108': 'Technology',  # Descartes
    '53115L104': 'Energy',  # Liberty Energy
    '861896108': 'Financial Services',  # StoneX
    '03152W109': 'Healthcare',  # Amicus
    'N72482156': 'Healthcare',  # QIAGEN
    '49803T300': 'Real Estate',  # Kite Realty
    '428103105': 'Energy',  # Hess Midstream
    '20459V105': 'Technology',  # CompoSecure
    '703481101': 'Energy',  # Patterson-UTI
    '185899101': 'Basic Materials',  # Cleveland-Cliffs
    '039653100': 'Industrials',  # Arcosa
    '530909308': 'Communication Services',  # Liberty Live
    '737446104': 'Consumer Defensive',  # Post Holdings
    '60786M105': 'Financial Services',  # Moelis
    '59001A102': 'Real Estate',  # Meritage
    '26210C104': 'Technology',  # Dropbox
    '059460303': 'Financial Services',  # Banco Bradesco
    '67000B104': 'Technology',  # Novanta
    '749607107': 'Financial Services',  # RLI
    '33767E202': 'Industrials',  # FirstService
    '808524847': 'Real Estate',  # Schwab US REIT ETF

    # ============ preset 机构特殊重仓补充 ============
    '451100101': 'Energy',  # Icahn Enterprises
    '12662P108': 'Energy',  # CVR Energy
    '046353108': 'Healthcare',  # AstraZeneca ADR
    '44267T102': 'Real Estate',  # Howard Hughes
    '531229722': 'Communication Services',  # Liberty Media (FWONK)
    '531229748': 'Communication Services',  # Liberty Media
    '531229854': 'Communication Services',  # Liberty Media
    '26856L103': 'Consumer Defensive',  # e.l.f. Beauty
    '45827U109': 'Technology',  # Intapp
    '09075V102': 'Healthcare',  # BioNTech
    'G89479102': 'Industrials',  # TORM
    'H17182108': 'Healthcare',  # CRISPR Therapeutics
    'V5633W109': 'Consumer Cyclical',  # MakeMyTrip
    '126633205': 'Basic Materials',  # CVR Partners
    'G9572D103': 'Technology',  # Webull
    '833635105': 'Basic Materials',  # SQM
    '530909100': 'Communication Services',  # Liberty Live
    '366505105': 'Consumer Cyclical',  # Garrett Motion
    '963320106': 'Consumer Cyclical',  # Whirlpool
    '155923105': 'Utilities',  # Centuri
    '07373V105': 'Healthcare',  # Beam Therapeutics
    '03945R102': 'Industrials',  # Archer Aviation
    '142152107': 'Healthcare',  # Caris Life Sciences (private, Other? keep Healthcare)
    '90184D100': 'Healthcare',  # Twist Bioscience
    'N30577105': 'Consumer Cyclical',  # Ermenegildo Zegna
    'G4766E116': 'Healthcare',  # Indivior
    '84921RAB6': 'Communication Services',  # Spotify USA
    '88025U109': 'Healthcare',  # 10X Genomics
    '433313103': 'Healthcare',  # Hinge Health
    '82983N108': 'Energy',  # Sitio Royalties
    'G16910120': 'Financial Services',  # Bullish
    '071734107': 'Healthcare',  # Bausch Health
    'G13311116': 'Communication Services',  # Brera Holdings
    '37940XAU6': 'Technology',  # Global Payments
    '45579U109': 'Healthcare',  # Indivior (alt)
    '52490G102': 'Healthcare',  # Legend Biotech
    '947002101': 'Financial Services',  # Wealthfront
    '45826J105': 'Healthcare',  # Intellia
    '98980A105': 'Industrials',  # ZTO Express
    '62482R107': 'Financial Services',  # Mr. Cooper
    '090043AF7': 'Technology',  # Bill Holdings
    '69331CAL2': 'Utilities',  # PG&E (alt)
    '76954AAB9': 'Consumer Cyclical',  # Rivian (alt)
    '472145AF8': 'Healthcare',  # Jazz Investments (Jazz Pharma)
    '75629V104': 'Healthcare',  # Recursion
    '477143101': 'Industrials',  # JetBlue
    '92337F107': 'Healthcare',  # Veracyte
    '15743P104': 'Consumer Cyclical',  # Chagee
    '46269C102': 'Communication Services',  # Iridium
    '20717MAB9': 'Technology',  # Confluent
    '25402DAB8': 'Technology',  # DigitalOcean (alt)
    '37637K108': 'Technology',  # Gitlab
    '47215PAJ5': 'Consumer Cyclical',  # JD.com (alt)
    '15189TBD8': 'Utilities',  # CenterPoint (alt)
    '69553P100': 'Technology',  # PagerDuty
    '345370CZ1': 'Consumer Cyclical',  # Ford (alt)
    '26210CAC8': 'Technology',  # Dropbox (alt)
    '24823R105': 'Healthcare',  # Denali
    '8676EP108': 'Consumer Defensive',  # SunOpta
    '482497104': 'Financial Services',  # KE Holdings
    '30063P105': 'Healthcare',  # Exact Sciences
    '302492103': 'Technology',  # Flywire
    '812215200': 'Financial Services',  # Seaport Entertainment
    '55024UAF6': 'Technology',  # Lumentum (alt)
    '500081104': 'Technology',  # Kodiak AI
    '78163D100': 'Financial Services',  # Runway Growth
    '42806J700': 'Industrials',  # Hertz
    'G61188101': 'Communication Services',  # Liberty Global
    '15677J108': 'Technology',  # Dayforce
    '610236101': 'Consumer Cyclical',  # Monro
    '92837L109': 'Energy',  # Vista Energy
    '26210CAD6': 'Technology',  # Dropbox (alt2)
    '04272N102': 'Healthcare',  # Arrivent
    'G98196101': 'Technology',  # WNS Holdings
    '09239BAF6': 'Technology',  # Blackline
    '477143AP6': 'Industrials',  # JetBlue (alt)
    '29788T103': 'Technology',  # E2open
    '70451X104': 'Financial Services',  # Payoneer
    '842587DZ7': 'Utilities',  # Southern (alt)
    '29445S100': 'Industrials',  # EquipmentShare
    '124830878': 'Real Estate',  # CBL & Associates
    '767292AB1': 'Technology',  # Riot Platforms (alt)
    '57164YAD9': 'Consumer Cyclical',  # Marriott Vacations
    '89346D107': 'Utilities',  # TransAlta
    'M2682V108': 'Technology',  # CyberArk
    '29786AAJ5': 'Consumer Cyclical',  # Etsy (alt)
    '349381103': 'Financial Services',  # Figure Technology
    '81663L200': 'Healthcare',  # GeneDx
    '644535106': 'Basic Materials',  # New Gold
    '68278B107': 'Technology',  # OneStream
    'M5216V106': 'Technology',  # Global-E Online
    '40054J109': 'Industrials',  # Grupo Aeromexico
    '80007P869': 'Energy',  # SandRidge
    '49427F108': 'Real Estate',  # Kilroy
    '86771W105': 'Technology',  # Sunrun
    '18915MAC1': 'Technology',  # Cloudflare (alt)
    '753422AF1': 'Technology',  # Rapid7
    '55024UAD1': 'Technology',  # Lumentum (alt2)
    '784305104': 'Utilities',  # H2O America
    '87161C501': 'Financial Services',  # Synovus
    '83304AAK2': 'Communication Services',  # Snap
    '98954M101': 'Real Estate',  # Zillow (alt)
    '69404D108': 'Healthcare',  # Pacific Biosciences
    '78464A888': 'Financial Services',  # State Street (alt)
    '03769M304': 'Financial Services',  # Apollo (alt)
    '200340107': 'Financial Services',  # Comerica
    '278768AB2': 'Communication Services',  # EchoStar (alt)
    '171757206': 'Healthcare',  # CIDARA
    '05370A108': 'Healthcare',  # Avidity
    '65339F655': 'Utilities',  # NextEra (alt)
    '071705107': 'Healthcare',  # Bausch + Lomb
    '922280AD4': 'Technology',  # Varonis
    '70509VAA8': 'Real Estate',  # Pebblebrook
    '985194208': 'Consumer Cyclical',  # Yatsen
    '252131AM9': 'Healthcare',  # Dexcom (alt)
    '12685JAE5': 'Communication Services',  # Cable One
    '91680M107': 'Financial Services',  # Upstart
    'G9001E128': 'Communication Services',  # Liberty Latin America (alt)
    '03040WBE4': 'Utilities',  # American Water Capital
    'Y8162K204': 'Industrials',  # Star Bulk
    '29786AAN6': 'Consumer Cyclical',  # Etsy (alt2)
    '09061GAK7': 'Healthcare',  # BioMarin (alt)
    '19260QAB3': 'Financial Services',  # Coinbase (alt)
    '00138L108': 'Consumer Cyclical',  # AtRenew
    '10922N103': 'Financial Services',  # Brighthouse
    '29355AAK3': 'Technology',  # Enphase (alt)
    '71535D106': 'Healthcare',  # Personalis
    '009066AB7': 'Consumer Cyclical',  # Airbnb (alt)
    '57164YAF4': 'Consumer Cyclical',  # Marriott Vacations (alt)
    '46125A100': 'Industrials',  # Intuitive Machines
    '87265H109': 'Real Estate',  # Tri Pointe
    '81141RAG5': 'Technology',  # Sea Ltd (alt)
    '00650F109': 'Healthcare',  # Adaptive Biotechnologies
    '732908108': 'Technology',  # Pony AI
    '679295AF2': 'Technology',  # Okta (alt)
    '08265TAD1': 'Technology',  # Bentley (alt)
    '088929104': 'Financial Services',  # BGC Group
    '27923Q109': 'Basic Materials',  # Ecovyst
    '204166102': 'Technology',  # Commvault
    '984245100': 'Energy',  # YPF
    '74736LAD1': 'Technology',  # Q2 Holdings
    '460690100': 'Communication Services',  # Interpublic
    '03209RAB9': 'Healthcare',  # Amphastar
    '26142RAB0': 'Consumer Cyclical',  # DraftKings (alt)
    '58463J304': 'Real Estate',  # Medical Properties Trust
    '911684108': 'Utilities',  # Array Digital Infrastructure
    '62886HBD2': 'Consumer Cyclical',  # Norwegian (alt)
    '207410AH4': 'Healthcare',  # CONMED
    '74767V109': 'Technology',  # QuantumScape
    '382550101': 'Consumer Cyclical',  # Goodyear
    '48553T106': 'Technology',  # Kanzhun
    'G6359F137': 'Energy',  # Nabors
    '87918AAF2': 'Healthcare',  # Teladoc
    '586001109': 'Consumer Cyclical',  # Soho House
    '852234AJ2': 'Technology',  # Block (alt)
    '743312AB6': 'Technology',  # Progress Software
    '23285D109': 'Healthcare',  # Cytek
    '038923108': 'Real Estate',  # Arbor Realty
    '279158109': 'Energy',  # Ecopetrol
    '670002401': 'Healthcare',  # Novavax
    '880890108': 'Basic Materials',  # Ternium
    '803607AD2': 'Healthcare',  # Sarepta (alt)
    '866082AA8': 'Real Estate',  # Summit Hotel
    '44952J104': 'Energy',  # Crescent Energy
    '83304AAH9': 'Communication Services',  # Snap (alt)
    '67401P405': 'Financial Services',  # Oaktree Specialty Lending
    '092667104': 'Healthcare',  # Strata Critical Medical
    '90364P105': 'Technology',  # UiPath
    '98877R104': 'Consumer Cyclical',  # ZKH Group
    '011642AB1': 'Technology',  # Alarm.com
    '44975P103': 'Healthcare',  # Novabridge
    '879273209': 'Communication Services',  # Telecom Argentina
    '389375106': 'Communication Services',  # Gray Media
    '25243Q205': 'Consumer Defensive',  # Diageo
    '29355AAH0': 'Technology',  # Enphase (alt2)
    'G9001E102': 'Communication Services',  # Liberty Latin America
    '526057302': 'Real Estate',  # Lennar (alt)
    '92940WAD1': 'Technology',  # Wix (alt)
    '29415FAD6': 'Healthcare',  # Envista (alt)
    '679295AD7': 'Technology',  # Okta (alt2)

    # ============ 其余 top 持仓 ============
    '00206R102': 'Communication Services',  # AT&T
    '92343V104': 'Communication Services',  # Verizon
    '20030N101': 'Communication Services',  # Comcast
    '30303M102': 'Communication Services',  # Meta
    '30303M952': 'Communication Services',  # Meta (alt)
    '16119P108': 'Communication Services',  # Charter
    '887936101': 'Communication Services',  # Omnicom
    '460690106': 'Communication Services',
    '92855M639': 'Communication Services',  # Live Nation? no
    '538034109': 'Communication Services',  # Live Nation
    '357738100': 'Communication Services',  # Interpublic? alt
    '668873102': 'Communication Services',  # Regeneron? no -> Healthcare
    '75886F107': 'Healthcare',  # Regeneron
    '26884L109': 'Financial Services',  # Equitable (alt)
    '863079109': 'Communication Services',  # Dish/EchoStar
    '278768106': 'Communication Services',  # EchoStar
    '92532F109': 'Healthcare',
    '74144T108': 'Financial Services',  # T. Rowe Price
    '576323109': 'Financial Services',  # MasTec
    '47103U845': 'Financial Services',  # Janus Detroit (bond)
    '955306105': 'Healthcare',  # West Pharmaceutical
    '37733W204': 'Healthcare',  # GSK
    '29362U104': 'Technology',  # Entegris
    '980745103': 'Industrials',  # Woodward
    '74350P691': 'Financial Services',  # ProShares
    'G3323L100': 'Technology',  # Fabrinet
    '878742204': 'Basic Materials',  # Teck Resources
    'G2519Y108': 'Financial Services',  # Credicorp
    '01741R102': 'Basic Materials',  # ATI
    '695156109': 'Basic Materials',  # Packaging Corp
    '144285103': 'Basic Materials',  # Carpenter Technology
    '110448107': 'Consumer Defensive',  # British American Tobacco
    '40434L105': 'Technology',  # HP Inc
    '74743L100': 'Technology',  # Qnity Electronics
    '12514G108': 'Technology',  # CDW
    'N00985106': 'Industrials',  # AerCap
    '00217D100': 'Communication Services',  # AST SpaceMobile
    '962166104': 'Basic Materials',  # Weyerhaeuser
    '90138F102': 'Technology',  # Twilio
    '460146103': 'Basic Materials',  # International Paper
    '34959J108': 'Industrials',  # Fortive
    '30034W106': 'Utilities',  # Evergy
    '436440101': 'Healthcare',  # Hologic
    '45104G104': 'Financial Services',  # ICICI Bank
    '881624209': 'Healthcare',  # Teva
    '45167R104': 'Industrials',  # IDEX
    '87256C101': 'Consumer Cyclical',  # TKO Group
    '98956P102': 'Healthcare',  # Zimmer Biomet
    '780287108': 'Basic Materials',  # Royal Gold
    '055622104': 'Energy',  # BP
    'G6700G107': 'Industrials',  # nVent
    '018802108': 'Utilities',  # Alliant (alt)
    '40415F101': 'Financial Services',  # HDFC Bank
    '65290E101': 'Utilities',  # NextPower
    '315616102': 'Technology',  # F5
    '83406F102': 'Financial Services',  # SoFi
    '26614N102': 'Basic Materials',  # DuPont
    '98980L101': 'Communication Services',  # Zoom
    '453038408': 'Energy',  # Imperial Oil
    '896239100': 'Technology',  # Trimble
    '902494103': 'Consumer Defensive',  # Tyson
    'N97284108': 'Technology',  # Nebius
    '74935Q107': 'Industrials',  # RB Global
    '445658107': 'Industrials',  # J.B. Hunt
    '517834107': 'Consumer Cyclical',  # Las Vegas Sands
    '05605H100': 'Industrials',  # BWX Technologies
    '62944T105': 'Real Estate',  # NVR
    '803054204': 'Technology',  # SAP
    '87422Q109': 'Utilities',  # Talen
    '89400J107': 'Financial Services',  # TransUnion
    '866796105': 'Financial Services',  # Sun Life
    '98980G102': 'Technology',  # Zscaler
    'G76279101': 'Healthcare',  # Roivant
    '71654V408': 'Energy',  # Petrobras
    '670100205': 'Healthcare',  # Novo Nordisk
    '03743Q108': 'Energy',  # APA
    '243537107': 'Consumer Cyclical',  # Deckers
    '74624M102': 'Technology',  # Pure Storage
    '525327102': 'Industrials',  # Leidos
    '88023U101': 'Industrials',  # Somnigroup
    '942622200': 'Industrials',  # Watsco
    '142339100': 'Industrials',  # Carlisle
    '45073V108': 'Industrials',  # ITT
    '04626A103': 'Technology',  # Astera Labs
    '775711104': 'Industrials',  # Rollins
    'G25839104': 'Consumer Defensive',  # Coca-Cola Europacific
    '75524B104': 'Industrials',  # RBC Bearings
    '45784P101': 'Healthcare',  # Insulet
    'N82405106': 'Consumer Cyclical',  # Stellantis
    '10806X102': 'Healthcare',  # BridgeBio
    '422806208': 'Industrials',  # HEICO
    '751212101': 'Consumer Cyclical',  # Ralph Lauren
    '55306N104': 'Technology',  # MKS Instruments
    '46187W107': 'Real Estate',  # Invitation Homes
    '086516101': 'Consumer Cyclical',  # Best Buy
    '013872106': 'Basic Materials',  # Alcoa
    '74251V102': 'Financial Services',  # Principal Financial
    '00827B106': 'Financial Services',  # Affirm
    '058498106': 'Basic Materials',  # Ball
    '98149E303': 'Basic Materials',  # iShares Gold Trust (World Gold Trust) -> ETF Precious Metals
    '91912E105': 'Basic Materials',  # Vale
    '253393102': 'Consumer Cyclical',  # Dick's
    '172573107': 'Financial Services',  # Circle Internet Group
    '35137L105': 'Communication Services',  # Fox Corp (alt)
    'G25457105': 'Technology',  # Credo Technology
    '69370C100': 'Technology',  # Parametric Technology
    '82846H405': 'Industrials',  # QXO
    '372460105': 'Consumer Defensive',  # Genuine Parts
    '55405Y100': 'Technology',  # MACOM
    '72352L106': 'Communication Services',  # Pinterest
    '349553107': 'Utilities',  # Fortis
    '71424F105': 'Energy',  # Permian Resources
    '558868105': 'Healthcare',  # Madrigal
    '366651107': 'Technology',  # Gartner
    '88033G407': 'Healthcare',  # Tenet
    '679295105': 'Technology',  # Okta
    'N3168P101': 'Industrials',  # Ferrovial
    '902252105': 'Technology',  # Tyler Technologies
    '216648501': 'Healthcare',  # Cooper Companies
    '59522J103': 'Real Estate',  # Mid-America Apartments
    '49446R109': 'Real Estate',  # Kimco
    '540424108': 'Financial Services',  # Loews
    '69047Q102': 'Energy',  # Ovintiv
    '46982L108': 'Industrials',  # Jacobs
    '768861101': 'Utilities',  # Pembina
    '48020Q107': 'Real Estate',  # JLL
    '526107107': 'Industrials',  # Lennox
    '71377A103': 'Consumer Defensive',  # Performance Food Group
    '78467J100': 'Technology',  # SS&C
    '31946M103': 'Financial Services',  # First Citizens
    '016255101': 'Healthcare',  # Align
    '16411Q101': 'Energy',  # Cheniere Partners
    '758849103': 'Real Estate',  # Regency Centers
    'G3265R107': 'Consumer Cyclical',  # Aptiv
    '189054109': 'Consumer Defensive',  # Clorox
    '98850P109': 'Consumer Cyclical',  # Yum China
    '446413106': 'Industrials',  # Huntington Ingalls
    '767204100': 'Basic Materials',  # Rio Tinto
    '053611109': 'Basic Materials',  # Avery Dennison
    'H2927K103': 'Basic Materials',  # Amrize
    '25754A201': 'Consumer Cyclical',  # Domino's
    '866674104': 'Real Estate',  # Sun Communities
    '05550J101': 'Consumer Defensive',  # BJ's Wholesale
    '418056107': 'Consumer Cyclical',  # Hasbro
    '04351P101': 'Healthcare',  # Ascendis
    '579780206': 'Consumer Defensive',  # McCormick
    'G93A5A101': 'Industrials',  # Viking Holdings
    '00187Y100': 'Industrials',  # API Group
    '64125C109': 'Healthcare',  # Neurocrine
    '883203101': 'Industrials',  # Textron
    'G7S00T104': 'Industrials',  # Pentair
    'G3223R108': 'Financial Services',  # Everest Group
    '380237107': 'Technology',  # GoDaddy
    '922908512': 'Financial Services',  # Vanguard Group? -> fund
    '33829M101': 'Consumer Cyclical',  # Five Below
    '759351604': 'Financial Services',  # Reinsurance Group
    '904767803': 'Consumer Defensive',  # Unilever
    '92556V106': 'Healthcare',  # Viatris
    '888787108': 'Technology',  # Toast
    '723484101': 'Utilities',  # Pinnacle West
    '95082P105': 'Industrials',  # WESCO
    '192108504': 'Basic Materials',  # Coeur
    '384109104': 'Industrials',  # Graco
    '09061G101': 'Healthcare',  # BioMarin
    'G0250X149': 'Basic Materials',  # Amcor
    '007973100': 'Technology',  # Advanced Energy
    '650111107': 'Communication Services',  # New York Times
    '27579R104': 'Financial Services',  # East West Bancorp
    '127190304': 'Industrials',  # CACI
    '759509102': 'Basic Materials',  # Reliance
    '758750103': 'Industrials',  # Regal Rexnord
    '55336V100': 'Energy',  # MPLX
    '84265V105': 'Basic Materials',  # Southern Copper
    '05534B760': 'Communication Services',  # BCE
    '44107P104': 'Real Estate',  # Host Hotels
    '462222100': 'Healthcare',  # Ionis
    '03852U106': 'Consumer Cyclical',  # Aramark
    '426281101': 'Technology',  # Henry Jack
    '40171V100': 'Technology',  # Guidewire
    '989207105': 'Technology',  # Zebra
    'G4124C109': 'Technology',  # Grab
    '36467J108': 'Real Estate',  # Gaming & Leisure
    '574599106': 'Industrials',  # Masco
    'G0176J109': 'Industrials',  # Allegion
    '77311W101': 'Financial Services',  # Rocket Companies
    '049468101': 'Technology',  # Atlassian
    '443573100': 'Technology',  # HubSpot
    '28414H103': 'Healthcare',  # Elanco
    '79589L106': 'Technology',  # Samsara
    '14316J108': 'Financial Services',  # Carlyle
    '94419L101': 'Consumer Cyclical',  # Wayfair
    '655663102': 'Industrials',  # Nordson
    '92936U109': 'Real Estate',  # W.P. Carey
    '889478103': 'Real Estate',  # Toll Brothers
    '42250P103': 'Real Estate',  # Healthpeak
    '268150109': 'Technology',  # Dynatrace
    '40131M109': 'Healthcare',  # Guardant
    '697900108': 'Basic Materials',  # Pan American Silver
    '902653104': 'Real Estate',  # UDR
    '03674X106': 'Energy',  # Antero Resources
    '77543R102': 'Communication Services',  # Roku
    '50077B207': 'Industrials',  # Kratos
    '88339J105': 'Technology',  # Trade Desk
    '02005N100': 'Financial Services',  # Ally
    '65249B109': 'Communication Services',  # News Corp
    'G7496G103': 'Financial Services',  # RenaissanceRe
    '882681109': 'Consumer Cyclical',  # Texas Roadhouse
    '89055F103': 'Industrials',  # TopBuild
    '607828100': 'Industrials',  # Modine
    '422806109': 'Industrials',  # HEICO (alt)
    'G50871105': 'Healthcare',  # Jazz Pharma
    '00130H105': 'Utilities',  # AES
    '228368106': 'Basic Materials',  # Crown Holdings
    '859241101': 'Industrials',  # Sterling Infrastructure
    '87162W100': 'Technology',  # TD Synnex
    '126117100': 'Financial Services',  # CNA
    '892672106': 'Financial Services',  # Tradeweb
    '368736104': 'Industrials',  # Generac
    '16679L109': 'Consumer Defensive',  # Chewy
    '23345M107': 'Energy',  # DT Midstream
    '58506Q109': 'Healthcare',  # Medpace
    '749685103': 'Basic Materials',  # RPM
    '783513203': 'Industrials',  # Ryanair
    '70975L107': 'Healthcare',  # Penumbra
    '099724106': 'Consumer Cyclical',  # BorgWarner
    'H50430232': 'Technology',  # Logitech
    '559222401': 'Consumer Cyclical',  # Magna
    '624756102': 'Industrials',  # Mueller Industries
    '29472R108': 'Real Estate',  # Equity Lifestyle
    '74006W207': 'Healthcare',  # Praxis
    '87971M103': 'Communication Services',  # Telus
    '31620R303': 'Financial Services',  # Fidelity National Financial
    '83088M102': 'Technology',  # Skyworks
    '237266101': 'Basic Materials',  # Darling
    '30161Q104': 'Healthcare',  # Exelixis
    '031162105': 'Healthcare',  # Amgen? no 031162100
    '053332102': 'Consumer Cyclical',  # AutoZone
    '828806109': 'Real Estate',  # Simon Property
    '56585A102': 'Energy',  # Marathon Petroleum
    '05722G100': 'Energy',  # Baker Hughes
    '253868103': 'Real Estate',  # Digital Realty
    '674599105': 'Energy',  # Occidental
    'G87052109': 'Technology',  # TE Connectivity
    '16411R208': 'Energy',  # Cheniere
    'H42097107': 'Financial Services',  # UBS
    '48251W104': 'Financial Services',  # KKR
    '934423104': 'Communication Services',  # Warner Bros Discovery
    '146869102': 'Consumer Cyclical',  # Carvana
    '722304102': 'Consumer Cyclical',  # PDD
    '49456B101': 'Energy',  # Kinder Morgan
    '61174X109': 'Consumer Cyclical',  # Monster Beverage
    '89417E109': 'Financial Services',  # Travelers
    '24703L202': 'Technology',  # Dell
    '03073E105': 'Healthcare',  # Cencora
    '052053103': 'Technology',  # ADP
    '571748102': 'Financial Services',  # Marsh & McLennan
    '09260D107': 'Financial Services',  # Blackstone
    '26875P101': 'Energy',  # EOG
    '969457100': 'Energy',  # Williams
    '03027X100': 'Real Estate',  # American Tower
    '842587107': 'Utilities',  # Southern
    '654106103': 'Consumer Cyclical',  # Nike
    'G51502105': 'Industrials',  # Johnson Controls
    '824348106': 'Basic Materials',  # Sherwin-Williams
    '806857108': 'Energy',  # SLB
    '58733R102': 'Consumer Cyclical',  # MercadoLibre
    '891160509': 'Financial Services',  # Toronto-Dominion
    '26441C204': 'Utilities',  # Duke Energy
    '693475105': 'Financial Services',  # PNC
    '02209S103': 'Consumer Defensive',  # Altria
    '842587109': 'Utilities',
    '31428X106': 'Industrials',  # FedEx
    'G25508105': 'Basic Materials',  # CRH
    '064058100': 'Financial Services',  # BNY Mellon
    '902973304': 'Financial Services',  # US Bancorp
    '609207105': 'Consumer Defensive',  # Mondelez
    'G0403H108': 'Financial Services',  # Aon
    '278865100': 'Basic Materials',  # Ecolab
    '67103H107': 'Consumer Cyclical',  # O'Reilly
    '231021106': 'Industrials',  # Cummins
    '91913Y100': 'Energy',  # Valero
    '125523100': 'Healthcare',  # Cigna
    'V7780T103': 'Consumer Cyclical',  # Royal Caribbean
    '40412C101': 'Healthcare',  # HCA
    '036752103': 'Healthcare',  # Elevance
    '43300A203': 'Consumer Cyclical',  # Hilton
    '452308109': 'Industrials',  # Illinois Tool Works
    '718546104': 'Energy',  # Phillips 66
    '194162103': 'Consumer Defensive',  # Colgate
    '620076307': 'Technology',  # Motorola Solutions
    '778296103': 'Consumer Cyclical',  # Ross Stores
    '37045V100': 'Consumer Cyclical',  # GM
    '893641100': 'Industrials',  # TransDigm
    '88579Y101': 'Industrials',  # 3M
    '025537101': 'Utilities',  # American Electric Power
    '009158106': 'Basic Materials',  # Air Products
    '816851109': 'Utilities',  # Sempra
    '008474108': 'Basic Materials',  # Agnico Eagle
    '55024U109': 'Technology',  # Lumentum
    '113004105': 'Financial Services',  # Brookfield Asset Management
    '126408103': 'Industrials',  # CSX
    '98978V103': 'Healthcare',  # Zoetis
    '01609W102': 'Consumer Cyclical',  # Alibaba
    '01609WBG6': 'Consumer Cyclical',  # Alibaba (alt)
    '03769M106': 'Financial Services',  # Apollo
    '571903202': 'Consumer Cyclical',  # Marriott
    '363576109': 'Financial Services',  # Arthur J. Gallagher
    '29250N105': 'Energy',  # Enbridge
    '911312106': 'Industrials',  # UPS
    '655663103': 'Industrials',
    '369550108': 'Industrials',  # General Dynamics
    '502431109': 'Industrials',  # L3Harris
    '443201108': 'Industrials',  # Howmet
    '35671D857': 'Basic Materials',  # Freeport-McMoRan
    '94106L109': 'Industrials',  # Waste Management
    '136385101': 'Energy',  # Canadian Natural Resources
    '02582Q109': 'Financial Services',  # Progressive? no 743315103
    '00287Y109': 'Healthcare',  # AbbVie
    '166764100': 'Energy',  # Chevron
    '060505104': 'Financial Services',  # Bank of America
    '478160104': 'Healthcare',  # J&J
    '931142103': 'Consumer Defensive',  # Walmart
    '22160K105': 'Consumer Defensive',  # Costco
    '742718109': 'Consumer Defensive',  # P&G
    '191216100': 'Consumer Defensive',  # Coca-Cola
    '91324P102': 'Healthcare',  # UnitedHealth
    '58933Y105': 'Healthcare',  # Merck
    '717081109': 'Healthcare',
    '532457108': 'Healthcare',  # Eli Lilly
    '46625H100': 'Financial Services',  # JPMorgan
    '30231G102': 'Energy',  # Exxon
    '084670702': 'Financial Services',  # Berkshire
    '084670108': 'Financial Services',  # Berkshire (CL A)
    '57636Q104': 'Financial Services',  # Mastercard
    '002824100': 'Healthcare',  # Abbott
    '375558103': 'Healthcare',  # Gilead
    '46120E602': 'Healthcare',  # Intuitive Surgical
    'G0593M107': 'Healthcare',  # AstraZeneca
    '907818108': 'Industrials',  # Union Pacific
    '438516106': 'Industrials',  # Honeywell
    '097023105': 'Industrials',  # Boeing
    '09857L108': 'Consumer Cyclical',  # Booking
    '235851102': 'Healthcare',  # Danaher
    '95040Q104': 'Real Estate',  # Welltower
    'H1467J104': 'Financial Services',  # Chubb
    '548661107': 'Consumer Cyclical',  # Lowe's
    '539830109': 'Industrials',  # Lockheed Martin
    '110122108': 'Healthcare',  # Bristol-Myers
    '101137107': 'Healthcare',  # Boston Scientific
    '863667101': 'Healthcare',  # Stryker
    '82509L107': 'Technology',  # Shopify
    'G5960L103': 'Healthcare',  # Medtronic
    '14040H105': 'Financial Services',  # Capital One
    '872590104': 'Communication Services',  # T-Mobile
    '701094104': 'Industrials',  # Parker Hannifin
    '12572Q105': 'Financial Services',  # CME
    '855244109': 'Consumer Cyclical',
    '780087102': 'Financial Services',  # RBC
    '958102105': 'Technology',  # Western Digital
    '58155Q103': 'Healthcare',  # McKesson
    '651639106': 'Basic Materials',  # Newmont
    '126650100': 'Healthcare',  # CVS
    '29444U700': 'Real Estate',  # Equinix
    '45866F104': 'Financial Services',  # ICE
    'F92124100': 'Energy',  # TotalEnergies
    '21037T109': 'Utilities',  # Constellation Energy
    '22788C105': 'Technology',  # CrowdStrike
    'G8994E103': 'Industrials',  # Trane
    '369550106': 'Industrials',
    '92556V107': 'Healthcare',
    '632307104': 'Healthcare',  # Natera
    '892356106': 'Consumer Defensive',  # Tractor Supply
    '754730109': 'Financial Services',  # Raymond James
    '237194105': 'Consumer Cyclical',  # Darden
    '294429105': 'Industrials',  # Equifax
    'N3167Y103': 'Consumer Cyclical',  # Ferrari
    'G39387108': 'Technology',  # GlobalFoundries
    '012653101': 'Basic Materials',  # Albemarle
    'H11356104': 'Consumer Defensive',  # Bunge
    '15101Q207': 'Technology',  # Celestica
    'G87110105': 'Energy',  # TechnipFMC
    '858119100': 'Basic Materials',  # Steel Dynamics
    '278768109': 'Communication Services',
    '11133T103': 'Technology',  # Broadridge
    '122017106': 'Consumer Cyclical',  # Burlington
    '773121108': 'Industrials',  # Rocket Lab
    '66987V109': 'Healthcare',  # Novartis
    'G0378L100': 'Basic Materials',  # AngloGold
    'N53745100': 'Basic Materials',  # LyondellBasell
    '60937P106': 'Technology',  # MongoDB
    'G8473T100': 'Healthcare',  # Steris
    '053484101': 'Real Estate',  # AvalonBay
    '75734B100': 'Communication Services',  # Reddit
    '459506101': 'Basic Materials',  # IFF
    '00971T101': 'Technology',  # Akamai
    '12541W209': 'Industrials',  # C.H. Robinson
    '76131D103': 'Consumer Cyclical',  # Restaurant Brands
    '969904101': 'Consumer Cyclical',  # Williams-Sonoma
    '92343E102': 'Technology',  # Verisign
    '745867101': 'Real Estate',  # PulteGroup
    '171340102': 'Consumer Defensive',  # Church & Dwight
    '29476L107': 'Real Estate',  # Equity Residential
    '64110D104': 'Technology',  # NetApp
    'H01301128': 'Healthcare',  # Alcon
    '983793100': 'Industrials',  # XPO
    '60770K107': 'Healthcare',  # Moderna
    '504922105': 'Healthcare',  # Labcorp
    '084423102': 'Financial Services',  # W.R. Berkley
    '14020W106': 'Financial Services',  # Capital Group Dividend
    '115236101': 'Financial Services',  # Brown & Brown
    '92338C103': 'Industrials',  # Veralto
    '293792107': 'Energy',  # Enterprise Products
    '526057104': 'Real Estate',  # Lennar
    '74834L100': 'Healthcare',  # Quest Diagnostics
    'G8267P108': 'Basic Materials',  # Smurfit Westrock
    '15135B101': 'Healthcare',  # Centene
    '550021109': 'Consumer Cyclical',  # Lululemon
    '04016X101': 'Healthcare',  # argenx
    '496902404': 'Basic Materials',  # Kinross
    '302130109': 'Industrials',  # Expeditors
    '76155X100': 'Healthcare',  # Revolution Medicines
    '231561101': 'Industrials',  # Curtiss-Wright
    '7591EP100': 'Financial Services',  # Regions
    '65473P105': 'Utilities',  # NiSource
    '452327109': 'Healthcare',  # Illumina
    '912008109': 'Consumer Defensive',  # US Foods
    'G3643J108': 'Consumer Cyclical',  # Flutter
    '370334104': 'Communication Services',  # General Mills? no -> Consumer Defensive
    '531229755': 'Communication Services',  # Liberty Media
    '125269100': 'Basic Materials',  # CF Industries
    '866966104': 'Industrials',  # Sunbelt Rentals
    '493267108': 'Financial Services',  # KeyCorp
    '570535104': 'Financial Services',  # Markel
    '030420106': 'Industrials',
    '05161G106': 'Industrials',  # CR Bard? skip
    '46187W108': 'Real Estate',
    '88744Y103': 'Real Estate',  # Vornado? alt
    '887328107': 'Industrials',
    '91913Y101': 'Energy',
    '039483102': 'Consumer Defensive',  # ADM
    '36266G107': 'Healthcare',  # GE HealthCare
    '25179M103': 'Energy',  # Devon
    '29084Q100': 'Industrials',  # EMCOR
    '98419M100': 'Industrials',  # Xylem
    '592688105': 'Healthcare',  # Mettler-Toledo
    '704326107': 'Technology',  # Paychex
    '09062X103': 'Healthcare',  # Biogen
    '446150104': 'Financial Services',  # Huntington Bancshares
    'G96629103': 'Financial Services',  # Willis Towers Watson
    '68902V107': 'Industrials',  # Otis
    '427866108': 'Consumer Defensive',  # Hershey
    '941848103': 'Healthcare',  # Waters
    '217204106': 'Industrials',  # Copart
    '925652109': 'Real Estate',  # VICI
    '030420103': 'Utilities',  # American Water Works
    '876030107': 'Consumer Cyclical',  # Tapestry (Coach)
    '92345Y106': 'Industrials',  # Verisk
    '30212P303': 'Communication Services',  # Expedia
    '466313103': 'Technology',  # Jabil
    '22266T109': 'Consumer Cyclical',  # Coupang
    '260003108': 'Industrials',  # Dover
    '56501R106': 'Financial Services',  # Manulife
    '58507V107': 'Healthcare',  # Medline
    '337932107': 'Utilities',  # FirstEnergy
    '12503M108': 'Financial Services',  # Cboe
    'G5494J103': 'Basic Materials',  # Linde (alt)
    '91307C102': 'Healthcare',  # United Therapeutics
    'D18190898': 'Financial Services',  # Deutsche Bank
    'H2906T109': 'Technology',  # Garmin
    '281020107': 'Utilities',  # Edison International
    '260557103': 'Basic Materials',  # Dow
    '143658300': 'Consumer Cyclical',  # Carnival
    '165167735': 'Energy',  # Expand Energy
    '879360105': 'Technology',  # Teledyne
    '30225T102': 'Real Estate',  # Extra Space
    '69351T106': 'Utilities',  # PPL
    '15189T107': 'Utilities',  # CenterPoint
    '50212V100': 'Financial Services',  # LPL
    '444859102': 'Healthcare',  # Humana
    '303250104': 'Technology',  # Fair Isaac
    '67077M108': 'Basic Materials',  # Nutrien
    '87165B103': 'Financial Services',  # Synchrony
    '21036P108': 'Consumer Defensive',  # Constellation Brands
    '46284V101': 'Industrials',  # Iron Mountain
    '127097103': 'Energy',  # Coterra
    '443510607': 'Industrials',  # Hubbell
    '233331107': 'Utilities',  # DTE
    '45841N107': 'Financial Services',  # Interactive Brokers
    '174610105': 'Financial Services',  # Citizens Financial
    '042068205': 'Technology',  # ARM
    '761152107': 'Healthcare',  # ResMed
    '125896100': 'Utilities',  # CMS Energy
    '681919106': 'Communication Services',  # Omnicom
    '693506107': 'Basic Materials',  # PPG
    '500754106': 'Consumer Defensive',  # Kraft Heinz
    '744320102': 'Financial Services',  # Prudential
    '03990B101': 'Financial Services',  # Ares Management
    '15135U109': 'Energy',  # Cenovus
    '88262P102': 'Energy',  # Texas Pacific Land
    '90384S303': 'Consumer Cyclical',  # Ulta
    '518439104': 'Consumer Defensive',  # Estee Lauder
    '922475108': 'Healthcare',  # Veeva
    '256746108': 'Consumer Defensive',  # Dollar Tree
    '147528103': 'Consumer Defensive',  # Casey's
    '023608102': 'Utilities',  # Ameren
    '29273V100': 'Energy',  # Energy Transfer
    'G3730V105': 'Financial Services',  # FTAI Aviation
    'Y2573F102': 'Technology',  # Flex
    '60937P108': 'Technology',
    '36262G106': 'Industrials',
    '679295106': 'Technology',
    '754730106': 'Financial Services',
    'D18190899': 'Financial Services',
    '294429106': 'Industrials',
    '345370861': 'Consumer Cyclical',
    '884903808': 'Industrials',  # Thomson Reuters
    '457669307': 'Energy',  # Shell
    '670346105': 'Basic Materials',  # Nucor
    '92939U106': 'Utilities',  # WEC Energy
    '46266C105': 'Healthcare',  # IQVIA
    '871829107': 'Consumer Defensive',  # Sysco
    '910047109': 'Industrials',  # United Airlines
    '494368103': 'Consumer Defensive',  # Kimberly-Clark
    '256677105': 'Consumer Defensive',  # Dollar General
    '45687V106': 'Industrials',  # Ingersoll Rand
    '744573106': 'Utilities',  # PSEG
    '00846U101': 'Healthcare',  # Agilent
    '209115104': 'Utilities',  # Con Edison
    '406216101': 'Energy',  # Halliburton
    '81141R100': 'Technology',  # Sea Ltd
    '192446102': 'Technology',  # Cognizant
    '05464C101': 'Industrials',  # Axon
    '98138H101': 'Technology',  # Workday
    '13321L108': 'Energy',  # Cameco
    '49177J102': 'Consumer Defensive',  # Kenvue
    '049560105': 'Utilities',  # Atmos
    '42824C109': 'Technology',  # Hewlett Packard Enterprise
    '629377508': 'Utilities',  # NRG
    '679580100': 'Industrials',  # Old Dominion
    '704543105': 'Technology',
    '336433107': 'Technology',  # First Solar
    '437076107': 'Consumer Cyclical',  # Home Depot
    '931142109': 'Consumer Defensive',
    '053797107': 'Healthcare',
    '25179M104': 'Energy',
    '03073E106': 'Healthcare',
    '166764101': 'Energy',
    '084670706': 'Financial Services',
    '717081104': 'Healthcare',
    '91324P103': 'Healthcare',
    '58933Y106': 'Healthcare',
    '17275R103': 'Technology',
    '149123105': 'Industrials',
    '369604302': 'Industrials',
    '718172105': 'Consumer Defensive',
    '949746105': 'Financial Services',
    '38141G106': 'Financial Services',
    '75513E103': 'Industrials',
    '883556103': 'Healthcare',
    '580135105': 'Consumer Cyclical',
    '025816105': 'Financial Services',
    '713448105': 'Consumer Defensive',
    '482480104': 'Technology',
    '458140105': 'Technology',
    '172967424': 'Financial Services',  # Citigroup
    '459200101': 'Technology',  # IBM
    '872540105': 'Consumer Cyclical',
    '031162100': 'Healthcare',  # Amgen
    '882508105': 'Technology',
    '002824105': 'Healthcare',
    '92343V105': 'Communication Services',
    '375558105': 'Healthcare',
    '254687105': 'Communication Services',
    '00206R105': 'Communication Services',
    '032095103': 'Technology',
    '808513107': 'Financial Services',
    '90353T105': 'Technology',
    '46120E105': 'Healthcare',
    '20825C105': 'Energy',
    '907818105': 'Industrials',
    '032654106': 'Technology',
    'G29183105': 'Industrials',
    '438516105': 'Industrials',
    '097023103': 'Industrials',
    '09857L105': 'Consumer Cyclical',
    '747525105': 'Technology',
    '235851105': 'Healthcare',
    '95040Q105': 'Real Estate',
    'H1467J105': 'Financial Services',
    '548661105': 'Consumer Cyclical',
    '539830105': 'Industrials',
    '00724F105': 'Technology',
    '040413205 ': 'Technology',
    '101137105': 'Healthcare',
    '863667105': 'Healthcare',
    '82509L105': 'Technology',
    'G5960L105': 'Healthcare',
    '14040H105 ': 'Financial Services',
    '872590105': 'Communication Services',
    '701094105': 'Industrials',
    '12572Q105 ': 'Financial Services',
    '037833105': 'Technology',
    '594918105': 'Technology',
    '023135105': 'Consumer Cyclical',
    '11135F105': 'Technology',
    '30303M105': 'Communication Services',
    '88160R105': 'Consumer Cyclical',
    '532457105': 'Healthcare',
    '46625H105': 'Financial Services',
    '30231G105': 'Energy',
    '084670705': 'Financial Services',
    '478160105': 'Healthcare',
    '57636Q105': 'Financial Services',
    '931142105': 'Consumer Defensive',
    '22160K105 ': 'Consumer Defensive',
    '742718105': 'Consumer Defensive',
    '191216105': 'Consumer Defensive',
    '91324P105': 'Healthcare',
    '58933Y105 ': 'Healthcare',
}

# ETF / 基金发行商关键词（这些 issuer 一律视为基金）
FUND_ISSUER_PREFIX = (
    'SPDR', 'ISHARES', 'VANECK', 'VAN ECK', 'KRANESHARES', 'WISDOMTREE',
    'GLOBAL X', 'PROCURE', 'XTRACK', 'DBX', 'GUGGENHEIM', 'FIRST TR',
    'INVESCO QQQ', 'DIMENSIONAL', 'ARK 21', 'SPDR SERIES', 'SELECT SECTOR',
    'STATE STREET SPDR', 'STATE STR SPDR', 'SPDR GOLD', 'ISHARES SILVER',
    'ISHARES BITCOIN', 'WORLD GOLD TR', 'SPROTT', 'DIREXION', 'PROSHARES',
    'VANGUARD', 'FIDELITY', 'SCHWAB', 'JPMORGAN EXCHANGE', 'J P MORGAN EXCHANGE', 'PIMCO',
    'PACER', 'FRANKLIN', 'NUVEEN', 'AVANTIS', 'T ROWE', 'BLACKROCK',
    'DOUBLELINE', 'GAMCO', 'ALPS', 'FEDERATED', 'SEI', 'WESTERN ASSET',
    'DWS', 'MERRILL', 'AMERICAN CENTURY', 'PUTNAM', 'AQR', 'DFA',
    'EATON VANCE', 'VICTORY', 'TORTOISE', 'LAZARD', 'PGIM', 'ALLSPRING',
    'GOLDMAN SACHS ETF', 'MORGAN STANLEY', 'INVESCO', 'PARAMETRIC',
    'EA SERIES', 'FLEXSHARES', 'CAPITAL GROUP', 'JOHN HANCOCK',
    'BLACKROCK ETF', 'BLACKROCK LIQUID', 'ISHARES', 'GUGGEN', 'AIM', 'BMO',
    'TURNTWIST', 'HORAN', 'VANECK', 'NOVA', 'ULTA', 'ONECHART',
)
# 运营公司白名单：issuer 与基金公司同前缀但实际是股票的
OPERATING_CO_OVERRIDE = {
    'JPMORGAN CHASE & CO.', 'JPMORGAN CHASE  CO', 'JPMORGAN CHASE  AND  CO',
    'J P MORGAN CHASE & CO', 'JPMORGAN CHASE & CO',
    'SCHWAB CHARLES CORP', 'THE CHARLES SCHWAB CORPORATION',
    'STATE STR CORP', 'STATE STREET CORP',
    'BLACKROCK INC', 'BLACKROCK FINANCE INC', 'BLACKROCK, INC.',
    'GOLDMAN SACHS GROUP INC', 'GOLDMAN SACHS GROUP INC/',
    'MORGAN STANLEY', 'MORGAN STANLEY  CO',
    'FIDELITY NATIONAL INFORMATION SERVICES', 'FIDELITY NATIONAL FINANCIAL',
    'INVESCO LTD', 'INVESCO CAPITAL',
    'FRANKLIN ELECTRONIC PUBLICATIONS', 'FRANKLIN COVEY CO',
    'T ROWE PRICE GROUP INC',
    'VANGUARD GROUP INC',
}
FUND_ISSUER_RE = re.compile(
    r'\b(FDS|FUND|FUNDS|TRUST|TR \d|ETF|INDEX F|EXCHANGE TRADED|SERIES|TARGET|PATHWAY|'
    r'SCOTTSDALE|CHARLOTTE|WHITEHALL|STAR FDS|MUN BD|BD INDEX|TAX-MANAGED|SPECIALIZED|'
    r'INTL EQUITY|PORTFOLIO|PORTFL|SELECT SECTOR|STRATEGIC|EQUITY ETF|QQQ|NASDAQ 100|'
    r'MSCI|S&P 500|S&P 500 ETF|RUSSELL|MSCI EMERGING|FTSE|ACTIVE ETF|ACTVELY MNGD)\b'
)
REIT_RE = re.compile(
    r'\b(REALTY|REIT|PROPERTIES|HOTELS?|APARTMENT|RESIDENT|STORAGE|TOWER|BANCORP|BANCSHARES)\b',
)

def is_fund(issuer: str, title: str) -> bool:
    """判断一条 13F 持仓是否为基金/ETF 而非运营公司股票。"""
    iu = (issuer or '').upper().strip()
    tu = (title or '').upper().strip()
    if iu in OPERATING_CO_OVERRIDE:
        return False
    if any(iu.startswith(p) for p in FUND_ISSUER_PREFIX):
        return True
    if FUND_ISSUER_RE.search(iu):
        # 排除 REIT / 银行运营公司
        if REIT_RE.search(iu):
            return False
        return True
    if 'ETF' in tu or 'TR UNIT' in tu or tu == 'GOLD SHS' or tu == 'UNIT SER 1' or tu == 'UT SER 1':
        return True
    return False


# ETF 细分类（参考 MacroMicro 图例）。规则按优先级从上到下匹配 issuer+title 文本。
ETF_CATEGORIES = [
    # (类别名, 匹配关键词列表)
    ('Precious Metals', ['GOLD', 'SILVER', 'PLATINUM', 'PRECIOUS']),
    ('High-Yield/Junk Bond', ['HI YLD', 'HIGH YIELD', 'HYLD', 'JUNK', 'IBOXX HI YD', 'HIGH-YIELD']),
    ('Investment Grade Corporate Bond', ['INV CP', 'INVESTMENT GRADE', 'CORP', 'CREDIT', 'IBOXX INV CP', 'IG']),
    ('Government Bond', ['TRSURY', 'TREASUR', 'TRSY', 'TREAS BD', 'TR BD', 'AGGBD', 'MBS', 'MUNI', 'MUN ', 'MUNICIPAL', 'NATIONAL MUN', 'TIPS', 'GOVT', 'GOVERNMENT', 'SOVEREIGN', 'ULTRA SHORT', 'SHORT-TERM', 'ULTRA SHRT', '1-3 MNTH', '0-3 MNTH', 'TOTAL BD', 'TOTAL BND', 'INT-TERM CORP', 'INT TERM CORP', 'INTER-TERM', 'BOND', 'BD ETF', 'BD MRKT', 'TR BD']),
    ('Broad Emerging Market', ['EMERGING', 'EMKT', 'EMR MKT', 'EMRG', 'EM EQUITY', 'CHINA', 'CSI CHI', 'MEXICO', 'BRAZIL', 'LATIN', 'LAT AM', 'KOREA', 'STH KOR', 'INDIA', 'TAIWAN', 'VIETNAM', 'MALAYSIA', 'INDONESIA', 'THAILAND', 'SINGAPORE', 'PHILIPPINE', 'ASIA EX', 'G4124', 'PONY']),
    ('Latin American Equity', ['LATIN AMERICA', 'MEXICO', 'BRAZIL', 'CHILE', 'PERU', 'COLOMBIA', 'ARGENTINA', 'LATAM']),
    ('Asia-Pacific (Emerging)', ['CHINA', 'CSI 300', 'MCHI', 'KWEB', 'ASIA-PACIFIC', 'GREATER CHINA', 'CHINESE', 'ASIA PACIFIC']),
    ('Broad Developed World', ['MSCI EAFE', 'EAFE', 'DEV MKT', 'DEVELOPED', 'DEV WORLD', 'INTL', 'INTERNATIONAL', 'GLOBAL EX', 'EUROPE', 'PACIFIC EX', 'FTSE DEV', 'CORE MSCI INTL', 'ALLWRLD', 'WORLD EX', 'TOTAL INTL']),
    ('Large Cap Blend', ['S&P 500', 'S&P500', 'S&P MID', 'MID CAP', 'MIDCAP', 'LARGE CAP', 'LRG CAP', 'CORE S&P', 'RUSSELL 1000', 'RUS 1000', 'RUSSELL 2000', 'RUSSELL 3000', 'TOTAL STK', 'TOTAL MARKET', 'WHOLE MARKET', 'VALUE ETF', 'GROWTH ETF', 'GRW ETF', 'VAL ETF', 'EQL WGT', 'EQUAL WEIGHT', 'MKT ETF', '500 ETF', 'MSCI USA', 'US EQUITY', 'CORE EQUITY', 'DIVIDEND', 'DIV APP', 'DIV GRWTH', 'BLEND', 'MEGA CP', 'TTL STK', 'STK MKT', 'US CORE', 'CORE 2', 'SNP', 'SP500', 'DOW JONES', 'INDL AVERAGE', 'NYSE ARCA', 'NIFTY', 'USA QLT', 'QUALITY', 'MOMENTUM', 'VALUE FACT', 'MULTI-FACTOR']),
]

def classify_etf(issuer: str, title: str) -> str:
    """把 ETF/基金文本粗分为 MacroMicro 图例中的类别。"""
    text = ((issuer or '') + ' ' + (title or '')).upper()
    # QQQ / 科技行业 ETF -> Large Cap Blend（或 Technology？MacroMicro 把行业 ETF 归为 Large Cap Blend）
    for cat, kws in ETF_CATEGORIES:
        for kw in kws:
            if kw in text:
                return cat
    return 'Large Cap Blend'
