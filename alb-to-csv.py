import os
import csv
import time
from datetime import datetime

# print(
# '''
#  ________  ________  ___      ___             ________  ________  ________   ___      ___ _______   ________  _________  ________  ________     
# |\   ____\|\   ____\|\  \    /  /|           |\   ____\|\   __  \|\   ___  \|\  \    /  /|\  ___ \ |\   __  \|\___   ___\\   __  \|\   __  \    
# \ \  \___|\ \  \___|\ \  \  /  / /___________\ \  \___|\ \  \|\  \ \  \\ \  \ \  \  /  / | \   __/|\ \  \|\  \|___ \  \_\ \  \|\  \ \  \|\  \   
#  \ \  \    \ \_____  \ \  \/  / /\____________\ \  \    \ \  \\\  \ \  \\ \  \ \  \/  / / \ \  \_|/_\ \   _  _\   \ \  \ \ \  \\\  \ \   _  _\  
#   \ \  \____\|____|\  \ \    / /\|____________|\ \  \____\ \  \\\  \ \  \\ \  \ \    / /   \ \  \_|\ \ \  \\  \|   \ \  \ \ \  \\\  \ \  \\  \| 
#    \ \_______\____\_\  \ \__/ /                 \ \_______\ \_______\ \__\\ \__\ \__/ /     \ \_______\ \__\\ _\    \ \__\ \ \_______\ \__\\ _\ 
#     \|_______|\_________\|__|/                   \|_______|\|_______|\|__| \|__|\|__|/       \|_______|\|__|\|__|    \|__|  \|_______|\|__|\|__|
#              \|_________|                                                                                                                                                                                                                                                                                                                                                                                                                      
# '''
# )

start_time = time.time()
current_date = datetime.strftime(datetime.now(), "%d-%m-%Y")
current_date_time = datetime.strftime(datetime.now(), "%d-%m-%Y %X")

base_dir = os.getcwd()

csv_fields = [
                'type',
                'time',
                'elb_resource_id',
                'client_address',
                'target_address',
                'request_processing_time',
                'target_processing_time',
                'response_processing_time',
                'elb_status_code',
                'target_status_code',
                'received_bytes',
                'sent_bytes',
                'request_verb',
                'request_url',
                'request_protocol',
                'ssl_cipher',
                'ssl_protocol',
                'target_group_arn',
                'trace_id',
                'domain_name',
                'chosen_cert_arn',
                'matched_rule_priority',
                'request_creation_time',
                'action_executed',
                'redirect_url',
                'lambda_error_reason',
                'target_port_list',
                'target_status_code',
                'classification',
                'classification_reason'
            ]
csv_file_dir = base_dir + '/csv/'
csv_file_name = 'alblog-' + current_date + '.csv'
csv_rows = []

log_file_dir = base_dir + '/alb-logs/'
log_file_list = os.listdir(log_file_dir)

if os.path.isdir(csv_file_dir) == False:
    print(
'''
{} does not exist.
creating {} directory.
'''.format(csv_file_dir, csv_file_dir)
)
    os.mkdir(csv_file_dir)

print('Total numbers of log files present for processing is {} files'.format(len(log_file_list)))

with open((csv_file_dir + csv_file_name), 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(csv_fields)

for file in log_file_list:
    log_file = open((log_file_dir + file), 'r')
    lines = log_file.readlines()
    log_file.close()

    for line in lines:
        line_split = line.split()
        del line_split[15:27]
        csv_rows.append(line_split)

    with open((csv_file_dir + csv_file_name), 'a') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(csv_rows)

    csv_rows.clear()

    print('[' + current_date_time + ']' + ' ' + file + ' file prcessed succesfully.')

end_time = time.time()
total_time = '{0:.3f}'.format(end_time - start_time)          

print(
'''all logs processed and saved in {}{} file.
Total time taken to process the log files is {} sec. 
'''.format(csv_file_dir, csv_file_name, total_time)
)
