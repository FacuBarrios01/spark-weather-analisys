import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

figure, axis = plt.subplots(4, 1)

def trimByMonths(df):
    lg = len(df['month'])
    for x in range(0, lg):
        df['month'][x] = df['month'][x][-2:]
    return df

def trimDates(df):
    lg = len(df['datetime'])
    for x in range(0, lg):
        df['datetime'][x] = df['datetime'][x][2:]
    return df

def generateMAVChart():
    dfMDA = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/madridMA/part-00000-26717868-9164-4b09-9ef8-0602105b7e93-c000.csv')
    dfMDA = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/madridMA/part-00000-26717868-9164-4b09-9ef8-0602105b7e93-c000.csv')
    dfVDA = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/valenciaMA/part-00000-a4bf45ba-f859-4e2f-ac25-a2b16a509dd9-c000.csv')
    dfMDA = trimDates(dfMDA)
    dfVDA = trimDates(dfVDA)

    dfMDA.rename(columns = {'datetime':'Months', 'avg(temp)':'AverageTemp'}, inplace = True)
    dfVDA.rename(columns = {'datetime':'Months', 'avg(temp)':'AverageTemp'}, inplace = True)
    
    axis[0].set_title('Monthly Temp. Averages')
    axis[0].set_ylabel('Temp. Cº')
    axis[0].set_xlabel('Time')
    
    axis[0].plot(dfMDA.Months, dfMDA.AverageTemp, color = 'blue',label = 'Madrid')
    axis[0].plot(dfVDA.Months, dfVDA.AverageTemp, color = 'orange', label = 'Valencia')
    axis[0].legend()

def generateWindAvgCharts():
    dfVWAV = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/valenciaWAV/part-00000-e12017c7-cc78-428b-a31b-19900592f8c3-c000.csv')
    dfMWAV = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/madridWAV/part-00000-a13c1515-b9e9-465d-821c-eb9332d686e9-c000.csv')
    dfMWAV = trimDates(dfMWAV)
    dfVWAV = trimDates(dfVWAV)

    axis[1].set_title('Monthly Wind Averages')
    axis[1].set_ylabel('Km/h')
    axis[1].set_xlabel('Time')
    axis[1].legend('upper right')
    
    axis[1].plot(dfMWAV.datetime, dfMWAV.windAverage,  color = 'blue', label = 'Madrid Wind Avg.')
    axis[1].plot(dfVWAV.datetime, dfVWAV.windAverage, color = 'orange', label = 'Valencia Wind Avg.')
    axis[1].plot(dfVWAV.datetime, dfMWAV.windVectorAvg, '--', color = 'orange', label = 'Valencia Wind Vector Avg.')
    axis[1].plot(dfMWAV.datetime, dfVWAV.windVectorAvg, '--', color = 'blue', label = 'Madrid Wind Vector Avg.')
    axis[1].legend()

def generateMinMaxTempCharts():
    dfV = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/valenciaMinMaxT/part-00000-6b0eaaa6-b0fe-4aef-a7a6-3be42e27994c-c000.csv')
    dfM = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/madridMinMaxT/part-00000-57239cd6-0d8b-49a7-96b6-878c98d1eef7-c000.csv')
    dfV = trimDates(dfV)
    dfM = trimDates(dfM)
    
    axis[2].set_title('Min. & Max. Temp.')
    axis[2].set_ylabel('Temp. Cº')
    axis[2].set_xlabel('Time') 

    axis[2].plot(dfV.datetime, dfV.tempMin, color = 'orange', label = 'Valencia Min. Temp.')
    axis[2].plot(dfV.datetime, dfV.tempMax, '--', color = 'orange', label = 'Valencia Max. Temp.')
    axis[2].plot(dfM.datetime, dfM.tempMin,  color = 'blue', label = 'Madrid Min. Temp.')
    axis[2].plot(dfM.datetime, dfM.tempMax, '--', color = 'blue', label = 'Madrid Max. Temp.')
    axis[2].legend()

def generateMaxDiffChart():
    df = pd.read_csv('/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output/monthlyMaxDif/part-00000-63a06690-608b-4993-b473-2bc867d0e68a-c000.csv')
    df['month'] = df['datetime']
    df.sort_values(by=['datetime'], inplace=True)
    df = trimByMonths(df)
    df['datetime'] = pd.to_datetime(df['datetime'])
    grouped = df.groupby(df['datetime'].map(lambda x: x.year))
    
    axis[3].set_title('Max. Temp. Difference')
    axis[3].set_ylabel('Cº')
    axis[3].set_xlabel('Time')

    grouped.sort
    print(grouped.get_group(2020)['max(difference)'])
    for x in grouped['month']:
        print(x)


    axis[3].plot(grouped.get_group(2020)['month'], grouped.get_group(2020)['max(difference)'], color = 'orange', label = '2020')
    axis[3].plot(grouped.get_group(2021)['month'], grouped.get_group(2021)['max(difference)'], color = 'blue', label = '2021')
    axis[3].plot(grouped.get_group(2022)['month'], grouped.get_group(2022)['max(difference)'], color = 'red', label = '2022')
    axis[3].legend()

if __name__ == "__main__":
    generateMAVChart()
    generateWindAvgCharts()
    generateMinMaxTempCharts()
    generateMaxDiffChart()
    plt.show()