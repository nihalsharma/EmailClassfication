from collections import OrderedDict
import getopt
import json
from operator import itemgetter
import sys
import asyncio
from os import listdir
from os.path import isfile, join
from classification.html_helper import create_html_files

MAX_DAYS = 30  # in case day range has to be checked for


def read_args(argv):
    """
    Reading command line arguments
    :param argv:
    :return:
    """
    input_dir, output_dir, days = '', '', MAX_DAYS
    try:
        opts, args = getopt.getopt(argv, "hi:o:d:", ["idir=", "odir=", "days="])
    except Exception as e:
        print("email_processor.py -i <inputfile> -o <outputfile> -d <days>")
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print("email_processor.py -i <inputdir> -o <outputdir> -d <days>")
            sys.exit()
        elif opt in ('-i', '-idir'):
            input_dir = arg
        elif opt in ('-o', '-odir'):
            output_dir = arg
        elif opt in ('-d', '-days'):
            days = arg
    return input_dir, output_dir, days


def pre_process_result(sorted_result_set):
    """
    Prepare result to be consumed for producing output html files
    :param sorted_result_set:
    :return:
    """
    result_set = []
    for result in sorted_result_set:
        d = {}
        d.update({'service': result[0]})
        d['name'] = result[1]['from_name']
        mails = result[1]['mails']  # tuples
        time_sorted_mails = sorted(mails, key=itemgetter(1), reverse=True)[:10]  # last 10 mails for each service
        mails = [m[0] for m in time_sorted_mails]  # getting sorted values
        d['mails'] = mails
        result_set.append(d)
    return result_set


@asyncio.coroutine
def prepare_output(sorted_result_set, output_dir):
    """
    Modify the result set and use it for html file fill up
    :param sorted_result_set:
    :param output_dir:
    """
    result_set = pre_process_result(sorted_result_set)
    yield from create_html_files(result_set, output_dir)


@asyncio.coroutine
def run_email_job(input_dir, output_dir, days):
    # get the input dir files
    """
    Run the email job
    1. Read the files from input directory - can be any other source - db, cached etc
    2. Prepare a dictionary which sort of merges the emails from the same sender
    3. Sort the result on the basis of count of emails from the sender (desc)
    4. Pick the first 10
    5. Create html files out of the final result set and put in a local directory
    :param input_dir:
    :param output_dir:
    :param days:
    """
    files = [f for f in listdir(input_dir) if isfile(join(input_dir, f))]
    if len(files) == 0:
        print("No input to work on")
        sys.exit()
    messages = yield from process_files(files, input_dir)
    # sort on the number of counts of emails from the sender
    sorted_messages = OrderedDict(sorted(messages.items(), key=lambda x: len(x[1]['mails']), reverse=True))
    sorted_list = list(sorted_messages.items())
    sorted_result_set = sorted_list[:10]  # picking the top 10
    yield from prepare_output(sorted_result_set, output_dir)


@asyncio.coroutine
def process_files(files, dir):
    """
    1. Read the files from input directory - can be any other source - db, cached etc
    2. Prepare a dictionary which sort of merges the emails from the same sender
    :param files:
    :param dir:
    :return:
    """
    messages = {}
    for f in files:
        with open(join(dir, f)) as config_file:
            content = json.load(config_file)
        if content:
            from_add, from_name = '', ''
            if len(content.get('messages')):
                for message in content.get('messages'):
                    if not message.get('sent') and message.get('inbox'):
                        from_add = message.get('from').get('e')
                        from_name = message.get('from').get('n')
                        # if senders are from gmail, yahoo, hotmail - may be personal emails
                        # and others would be apps and services - so split to get the service name
                        if not ('gmail' in from_add or 'yahoo' in from_add or 'hotmail' in from_add):
                            from_add = from_add.rsplit('@')[1]
                        break
            if from_add:
                if messages.get(from_add):
                    mails = messages[from_add]['mails']
                    if mails and len(mails):
                        mails.append((content.get('subject'), content.get('internalDate')))
                    else:
                        mails = [(content.get('subject'), content.get('internalDate'))]
                    messages[from_add]['mails'] = mails
                else:
                    messages.update({from_add: {}})
                    messages[from_add]['mails'] = [(content.get('subject'), content.get('internalDate'))]
                    messages[from_add]['from_name'] = from_name

    return messages


if __name__ == "__main__":
    input_dir, output_dir, days = read_args(sys.argv[1:])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_email_job(input_dir, output_dir, days))
    loop.run_forever()
