# market_map rebuild – validation report

Build: 2026-11-13T09:59:59Z  ·  locations: 3,188  ·  pairs re-mapped from legacy market_map

## Before vs after (same checks on legacy and rebuilt points)

| check | legacy market_map | rebuilt |
|---|---:|---:|
| has_point | 3,172 | 3,188 |
| missing | 16 | 0 |
| outside_country | 200 | 0 |
| shared_point | 287 | 0 |
| port_misplaced | 34 | 12 |
| any_issue | 362 | 12 |

Checks: `outside_country` = beyond 10 km (land) / 30 km (ports) of the Natural Earth polygon; `shared_point` = same point (~100 m) used by differently named places; `port_misplaced` = ocean port > 50 km from every port-gazetteer position, or > 40 km from the coast with no gazetteer entry.

## Status

| status | locations | records |
|---|---:|---:|
| OK | 3,171 | 891,380 |
| REVIEW | 17 | 522 |

## Coordinate source

| geo_src | high | medium | low | total |
|---|---:|---:|---:|---:|
| manual | 21 | 0 | 0 | 21 |
| unlocode | 232 | 2 | 0 | 234 |
| searoute | 139 | 3 | 0 | 142 |
| seaports | 7 | 1 | 0 | 8 |
| legacy | 2,428 | 17 | 3 | 2,448 |
| postal | 150 | 6 | 5 | 161 |
| postal_prefix | 0 | 96 | 5 | 101 |
| city | 0 | 73 | 0 | 73 |

## Review reasons

| reason | locations |
|---|---:|
| low_confidence | 13 |
| unresolved_disagreement | 11 |
| port_inland | 3 |

## Ports

- LOCODE locations: 413; without UN/LOCODE coordinates: 175; still without any coordinate: 0
- UN/LOCODE rejected (outside country or outvoted by port gazetteers): 1
- Port gazetteers used: searoute (3,923 codes), seaports (1,610 codes) — NGA World Port Index not available at build time

## Movement vs legacy

| moved | locations |
|---|---:|
| 0–1 km | 2,499 |
| 1–10 km | 274 |
| 10–50 km | 40 |
| 50–500 km | 96 |
| 500–∞ km | 263 |

### Largest corrections (moved ≥ 50 km, top 40 by records)

| cc | loc | legacy src | moved km | new src | legacy issue |
|---|---|---|---:|---|---|
| IN | INNSA | locode_dict | 446 | searoute | port |
| CN | CNTXG | locode_dict | 1,747 | unlocode | port |
| CN | CNHUA | locode_dict | 1,738 | searoute | port |
| CN | CNYSA | locode_dict | 73 | manual |  |
| CN | CNQZH | locode_dict | 1,084 | searoute | port |
| PA | PAMIT | locode_dict | 63 | searoute | port |
| SA | SAKAC | locode_dict | 1,206 | unlocode | port |
| DE | LUDWIGSHAFEN, 67056 | postal | 795 | postal | outside,shared |
| IN | INKAT | locode_dict | 452 | unlocode | port |
| IN | INTKD | locode_dict | 2,197 | manual |  |
| DE | MARL, 45764 | postal | 622 | postal_prefix | outside,shared |
| CR | CRMOB | locode_dict | 128 | unlocode | port |
| CN | CNDCB | locode_dict | 1,869 | unlocode | port |
| TR | Gebze, 41480 | postal | 10,231 | postal_prefix | outside,shared |
| MX | SAN JOSE ITURBIDE, 37989 | postal | 219 | postal_prefix | shared |
| GU | GUGUM | locode_geocode | 2,119 | seaports | outside,port |
| GB | PONTYCLUN, CF2 8YZ | postal | 219 | postal_prefix | shared |
| EC | ECPSJ | locode_dict | 90 | unlocode | port |
| CN | CNYPG | locode_dict | 614 | unlocode | shared,port |
| DE | NEUSS, 41453 | postal | 609 | postal | outside,shared |
| TH | THLKR | locode_dict | 129 | unlocode | port |
| BR | BRIOA | locode_dict | 101 | unlocode | port |
| TR | GAZIANTEP, 27180 | postal | 10,970 | postal_prefix | outside,shared |
| CN | CNXIP | locode_dict | 1,394 | searoute | port |
| DE | ASCHERSLEBEN, 6449 | postal | 6,219 | postal | outside |
| GB | GRAYS, RM17 5DU | postal | 280 | postal | shared |
| GB | CASTLEFORD, WF10 2JT | postal | 264 | postal | shared |
| BZ | BZBZE | locode_dict | 67 | searoute | port |
| DE | FRANKFURT, 65926 | postal | 772 | postal_prefix | outside,shared |
| US | USCHT | locode_dict | 1,027 | manual | port |
| NL | ROTTERDAM, 3000 CM | postal | 7,180 | city | outside,shared |
| PT | MATOSINHOS, 4460-020 | postal | 276 | postal | shared |
| TR | TREYP | locode_dict | 155 | unlocode | port |
| FR | ANDREZIEUX CEDEX, 42163 | postal | 212 | postal_prefix |  |
| US | SAINT PAUL, 55144 | postal | 2,543 | postal |  |
| VN | VNCLI | locode_dict | 1,135 | unlocode | port |
| US | PLAQUEMINE, 70765 | postal | 1,612 | postal |  |
| SE | PERSTORP, 28480 | postal | 5,759 | postal | outside,shared |
| US | ATLANTA, 30384 | postal | 982 | postal |  |
| US | EVANSVILLE, 47706 | postal | 922 | postal | shared |

### Manual overrides applied

| cc | loc | lat | lon | moved vs legacy km | note |
|---|---|---:|---:|---:|---|
| AS | ASPPG | -14.2766 | -170.69 | 1 | UN/LOCODE 1416N has the wrong hemisphere; Pago Pago harbour is 14.28S |
| BE | ROTTERDAM, 3195 ND | 51.885 | 4.385 | 146 | 3195 ND = Pernis, Rotterdam; source country BE is wrong |
| CN | CNXGG | 39.0 | 117.7 | 1,849 | Tianjin Xingang (Binhai New Area); 'Binhai' city name is ambiguous |
| CN | CNYSA | 30.63 | 122.07 | 73 | Yangshan deep-water port (Shanghai); no UN/LOCODE or gazetteer position |
| CN | CNYTN | 22.58 | 114.28 | 2 | UN/LOCODE 2651N 11952E is in Fujian; Yantian port is in Shenzhen |
| DE | KISTA, 16440 | 59.403 | 17.944 | 1,448 | Kista 164 40, Stockholm; source country DE is wrong |
| DE | LE GRAND SACONNEX, 01218 | 46.232 | 6.121 | 905 | Le Grand-Saconnex 1218, Geneva; source country DE is wrong |
| EG | 10TH RAMADAN, 44916 | 30.29 | 31.75 | 10,534 | 10th of Ramadan City - approximate city centre |
| EG | 6TH OCTOBER CITY, 001101 | 29.97 | 30.95 | 10,506 | 6th of October City - approximate city centre |
| EG | EGDEK | 31.133 | 29.8 | 2 | El Dekheila port (Alexandria), same port as EGEDK |
| FR | Lavera, 13117 | 43.389 | 5.005 | 615 | Lavera (Martigues) petrochemical port; legacy point was Strasbourg |
| FR | SALIN-DE-GIRAUD, 13129 | 43.412 | 4.731 | 620 | Salin-de-Giraud (Arles); legacy point was Strasbourg |
| IC | ICDN EMBAKASI | -1.318 | 36.88 |  | Embakasi ICD, Nairobi; source country code 'IC' is a parsing artefact |
| IN | INTKD | 28.506 | 77.269 | 2,197 | Tughlakabad ICD, New Delhi - inland container depot, inland by design |
| MX | LAREDO | 27.4763 | -99.5164 | 875 | Laredo border crossing, MX side = Nuevo Laredo; legacy point was in Michoacan |
| MX | Laredo,78040 | 27.4763 | -99.5164 | 895 | 78040 is a Laredo TX ZIP; MX side of the crossing = Nuevo Laredo |
| PK | PKQCT | 24.7814 | 67.3594 | 0 | Qasim International Container Terminal, Port Qasim (not Karachi city) |
| SE | HELSINKI, 00160 | 60.165 | 24.96 | 6,158 | Helsinki 00160 (Katajanokka); source country SE is wrong |
| TR | ERGENE, 00000 | 41.195 | 27.625 | 10,101 | Ergene district (Velimese), Tekirdag - approximate centre |
| TR | TREVY | 40.7667 | 29.7 | 26 | UN/LOCODE 4002N 04303E is in eastern Anatolia; Evyapan = Evyap Port Kocaeli (TREYP) |
| US | USCHT | 39.8476 | -75.3577 | 1,027 | Chester, PA (Delaware River); legacy point was Chattanooga, TN |

### Largest legacy shared points (geocoder fallbacks)

| lat | lon | locations | examples |
|---:|---:|---:|---|
| 30.048 | -94.336 | 57 | BEIJING, 101149, BEIJING, 102308, CANGZHOU, 061108, CHANGSHU, 215522 |
| 19.433 | -99.133 | 19 | COYOTEPEC , 54607, Cuernavaca, 62500, ECATEPEC DE MORELOS, 55330, ECATEPEC, 55330 |
| 37.606 | -82.324 | 18 | BECANCOUR, G9H 3X3, BRANTFORD, N3V 1G2, GATINEAU, J8P 7G7, KNOWLTON, J2K 3Y5 |
| 46.234 | 5.354 | 17 | AMIENS CEDEX 3, 80094, AMIENS, 80013, CHILLY MAZARIN CEDEX, 91385, DOUARNENEZ CEDEX, 29177 |
| 52.481 | -1.905 | 15 | Burgkirchen, 84504, COLOGNE, 50569, FRANKFURT, 65926, HANAU, 93457 |
| 38.675 | -0.81 | 11 | BRAMPTON, L6T 4W7, BRAMPTON, L6T 4Y4, BROMONT, J2L 1S6, BURLINGTON, L7N 3J5 |
| 34.394 | -100.894 | 10 | ERGENE, 00000, GAZIANTEP, 27180, GEBZE, 41470, GEBZE, 41480 |
| 51.503 | -0.001 | 8 | CASTLEFORD, WF10 2JT, COVENTRY, CV6 5RZ, Immingham, 1469, LEATHERHEAD, KT22 7UD |
| 38.894 | -77.043 | 7 | CHICAGO, 60674, CHICAGO, 60701, CINCINNATI, 45263, EVANSVILLE, 47706 |
| 23.605 | 58.43 | 7 | DAMMAM, 31412, JEDDAH, 21473, JEDDAH, 22427, RABIGH, 21911 |
| 53.463 | -2.237 | 6 | BALLYMENA, BT43 5DU, CHESHIRE, WA7 4JE, GRAYS, RM17 5DU, HALIFAX, HX4 9DL |
| 38.239 | -91.306 | 6 | AKASHISHI, HYOGO, 6740093, ANNAKA, 3790195, GUNMA, 3740111, HIRAKATA, 5730136 |
| 52.06 | -0.347 | 6 | CARRIGTWOHILL, T45 KW29, DROGHEDA, A92 PX62, DUBLIN 12, D12 T688, NAAS, Y91 YT92 |
| 40.992 | -75.908 | 6 | HIMEJI, 6711282, Himeji, 671-1282, ISONA, ANNAKA, 379-0127, Kawasaki, 210-0865 |
| 35.868 | -90.946 | 5 | 10TH RAMADAN, 44916, 6TH OCTOBER CITY, 001101, AIN SOKHNA, 43527, ALEXANDRIA, 12445 |
