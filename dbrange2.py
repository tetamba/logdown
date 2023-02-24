from datetime import datetime, timedelta
import pandas as pd

offline_at = '2023-01-15 16:04:01'
stop_at = '2023-01-23 06:58:51'

# date_range = [offline_at, stop_at]
# start, end = [datetime.fromisoformat(d) for d in date_range]

# output = [[start.date()+timedelta(d), start.date()+timedelta(d+1)] for d in range((end-start).days + 2)]
# output[0][0], output[-1][-1] = start, end

# output = [(l[0].strftime('%Y-%m-%d %H:%M'), l[1].strftime('%Y-%m-%d %H:%M')) for l in output]

# print(output)

# ini kode yang terpakai
# start = datetime.strptime(offline_at, "%Y-%m-%d %H:%M:%S")
# end = datetime.strptime(stop_at, "%Y-%m-%d %H:%M:%S")

# dates = [datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S") for date in pd.date_range(start.date() + timedelta(1), end.date())] + [start, end]
# dates = sorted(dates)

# diapason = []
# for i in range(len(dates)-1):
#     diapason.append((dates[i], dates[i+1]))

# print(diapason)

# generate date list
datelist = pd.date_range('2023-1-1','2023-1-31').strftime("%Y-%m-%d").tolist()
id = [101,201,301]
df = pd.DataFrame(id)
df["dates"] = [datelist] * len(df)
df = df.explode("dates", ignore_index=True)
# print(df)
# df.to_csv("id_df.csv", sep=',', encoding='utf-8')
