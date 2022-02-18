import re
import requests
import io
from zipfile import ZipFile
from datetime import datetime, timedelta
import argparse


def unzipFile(zip_file):
    ret = None
    myzip = ZipFile(io.BytesIO(zip_file))
    #enc = chardet.detect(myzip.read(myzip.namelist()[0]))['encoding']

    ret = myzip.read(myzip.namelist()[0]).decode("iso-8859-1")

    myzip.close()

    return ret

eng_master_file_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
transling_master_file_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
en_event_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip"
translingual_event_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.translation.export.CSV.zip"

en_mention_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.mentions.CSV.zip"
translingual_mention_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.translation.mentions.CSV.zip"


en_gkg_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip"
translingual_gkg_url_to_f = "http://data.gdeltproject.org/gdeltv2/{}.translation.gkg.csv.zip"

en_master_file = requests.get(eng_master_file_url)
transling_master_file_url = requests.get(transling_master_file_url)

regex_file_2021 = "http:\/\/data.gdeltproject.org\/gdeltv2\/2021.+"
url_2021 = re.findall(regex_file_2021, en_master_file.text) + re.findall(regex_file_2021, transling_master_file_url.text)

day = datetime(2021, 1, 1)
endDate = datetime(2021, 2, 1)
period = timedelta(minutes=15)
eventcsv_1 = io.StringIO()
eventcsv_2 = io.StringIO()
eventcsv_4 = io.StringIO()
mentioncsv_4 = io.StringIO()
mentioncsv_1 = io.StringIO()
mentioncsv_2 = io.StringIO()
gkgcsv_4 = io.StringIO()
gkgcsv_3 = io.StringIO()

def event_process_1(event):
    eventColIndx_1 = [0, 1, 33, 53]

    for i in event:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in eventColIndx_1])
        eventcsv_1.write(outputLine+'\n')

def event_process_2(event):
    eventColIndx_2 = [0, 1, 53]      

    for i in event:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in eventColIndx_2])
        eventcsv_2.write(outputLine+'\n')

def event_process_4(event):
    eventColIndx_4 = [0, 1, 7, 17]    

    for i in event:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in eventColIndx_4])
        eventcsv_4.write(outputLine+'\n')

def mention_process_1(mention):
    mentionColIndx_1 = [0, 14]

    for i in mention:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in mentionColIndx_1])
        mentioncsv_1.write(outputLine+'\n')

def mention_process_2(mention):
    mentionColIndx_2 = [0]

    for i in mention:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in mentionColIndx_2])
        mentioncsv_2.write(outputLine+'\n')

def mention_process_4(mention):
    mentionColIndx_4 = [0, 3, 4]

    for i in mention:
        line = i.split('\t')
        outputLine = ','.join([line[j] for j in mentionColIndx_4])
        mentioncsv_4.write(outputLine+'\n')

def gkg_process_4(gkg):

    def getValue(line, i):
        r = None
        if i == 15:
            r = line[i].split(',')[0]
        else:
             r = line[i]
        return r
    gkgColIndx_4 = [2, 3, 7, 15]

    for i in gkg:
        line = i.split('\t')
        outputLine = ','.join([getValue(line, j) for j in gkgColIndx_4 if len(line) == 27])
        gkgcsv_4.write(outputLine+'\n')

def gkg_process_3(gkg):

    def getValue(line, i):
        r = None
        if i == 15:
            r = line[i].split(',')[0]
        elif i == 9:
            try:
                r = line[i].split(';')[0].split('#')[2]
            except Exception:
                r = ''
        else:
             r = line[i]
        return r
    gkgColIndx_3 = [1, 3, 7, 9, 11, 15]
    for i in gkg:
        line = i.split('\t')
        outputLine = ','.join([getValue(line, j) for j in gkgColIndx_3 if len(line) == 27])
        gkgcsv_3.write(outputLine+'\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("begin", help="begin date")
    parser.add_argument("end", help="end date")
    args = parser.parse_args()

    day = datetime.strptime(args.begin, "%Y%m%d%H%M%S")
    endDate = datetime.strptime(args.end, "%Y%m%d%H%M%S")


    
    if day >= endDate:
       raise Exception

    while (day < endDate):
        en_event_url = en_event_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))
        translingual_event_url = translingual_event_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))
        event = unzipFile(requests.get(en_event_url).content).splitlines() + unzipFile(requests.get(translingual_event_url).content).splitlines()

        en_mention_url = en_mention_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))
        translingual_mentioin_url = translingual_mention_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))  
        mention =   unzipFile(requests.get(en_mention_url).content).splitlines() + unzipFile(requests.get(translingual_mentioin_url).content).splitlines()

        en_gkg_url = en_gkg_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))
        translingual_gkg_url = translingual_gkg_url_to_f.format(day.strftime("%Y%m%d%H%M%S"))  
        gkg =   unzipFile(requests.get(en_gkg_url).content).splitlines() + unzipFile(requests.get(translingual_gkg_url).content).splitlines()

        event_process_1(event)
        event_process_2(event)
        event_process_4(event)
        mention_process_1(mention)
        mention_process_2(mention)
        mention_process_4(mention)
        gkg_process_4(gkg)
        gkg_process_3(gkg)
        day += period

f= open("event_1_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(eventcsv_4.getvalue())
f.close()
f= open("event_2_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(eventcsv_2.getvalue())
f.close()
f= open("event_1_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(eventcsv_4.getvalue())
f.close()
f= open("mention_1_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(mentioncsv_1.getvalue())
f.close()
f= open("mention_2_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(mentioncsv_2.getvalue())
f.close()
f= open("mention_4_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(mentioncsv_4.getvalue())
f.close()
f= open("gkg_4_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(gkgcsv_4.getvalue())
f.close()
f= open("gkg_3_{}_{}.csv".format(args.begin, args.end), 'w')
f.write(gkgcsv_3.getvalue())
f.close()


