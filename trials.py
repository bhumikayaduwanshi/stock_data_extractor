from pathlib import Path
from config.config import US_25, NIFTY_50
from data.data_extraction.calculate_ratios import *
from config.config import location_list_IND, CONFIG


IndexFile = pd.read_csv(
    Path(CONFIG.get('directory').get('extraction').get('Nifty_50')))
print(IndexFile)
# Read Symbol From CSV FILE
Symbols = IndexFile["Symbol"].to_list()
StocksList = [i+".ns" for i in Symbols]
CompanyName = [i.replace(" ", "-").lower().strip('.')
               for i in IndexFile['Company Name'].values.tolist()]
id = IndexFile['ID'].to_list()
RATIO_DF = pd.DataFrame()

for i in range(len(Symbols)):
    ratio_table = extractRatio(CompanyName[i], Symbols[i], id[i])
    print(Symbols[i], len(ratio_table))
    ratio_df = ratio([5, 8], ratio_table, Symbols[i])
    print(ratio_df)
    print('---------------------------------------------')
    RATIO_DF = pd.concat([RATIO_DF, ratio_df], axis=0)


# ratio_table = extractRatio('titan-company-ltd', 'TITAN', 500114)
# print('TITAN', len(ratio_table))
# # ratio_df = ratio([5, 8], ratio_table, 'TITAN')
# # print(ratio_df)

# for i in range(len(ratio_table)):
#     print(i, ratio_table[i])
#     print('------------------------------')


# # ratio_table = extractRatio(
# #     'adani-ports-and-special-economic-zone-ltd', 'ADANIPORTS', 532921)
# # print('TITAN', len(ratio_table))
# # # ratio_df = ratio([5, 8], ratio_table, 'TITAN')
# # # print(ratio_df)

# # for i in range(len(ratio_table)):
# #     print(i, ratio_table[i])
# #     print('------------------------------')
