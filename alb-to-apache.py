import os
import time
from datetime import datetime
from sys import exit

start_time = time.time()
current_date = datetime.strftime(datetime.now(), "%d-%m-%Y")
current_date_time = datetime.now().strftime("%y%m%d_%H%M%S")

base_dir = os.getcwd()

apache_log_dir = base_dir + '/apache-logs/'
apache_log_file = apache_log_dir + 'ezdata_{}.log'.format(current_date_time)

log_file_dir = base_dir + '/alb-logs/'

if os.path.isdir(log_file_dir) == False:
    print('''{} does not exist.
creating {} directory.'''.format(log_file_dir, log_file_dir)
)
    os.mkdir(log_file_dir)

log_file_list = os.listdir(log_file_dir)

if len(log_file_list) <= 0:
    print('''No log files are present {} for processing.
exiting the script!'''.format(apache_log_dir))
    print('Please place all the load balancer log files into {} directory.'.format(log_file_dir))
    exit()

print('Total numbers of log files present for processing is {} files'.format(len(log_file_list)))

if os.path.isdir(apache_log_dir) == False:
    print('''{} does not exist.
creating {} directory.'''.format(apache_log_dir, apache_log_dir)
)
    os.mkdir(apache_log_dir)

for file in log_file_list:
    log_file = open((log_file_dir + file), 'r')
    lines = log_file.readlines()
    log_file.close()

    for i in lines:
        line_split = i.split()

        date_time = line_split[1]
        formatted_date = date_time[5:7] + '/' + date_time[8:10] + '/' + date_time[2:4]
        formatted_time = ' ' + date_time[11:19]
        main_date_time = formatted_date + formatted_time

        main_session_id = ' - '

        client_ip = line_split[3]
        main_client_ip = (client_ip.split(':')[0]) + ' '

        main_resp_bytes = '0 '

        req_serve_time = line_split[5:8]
        main_req_serve_time = str(
                                    (float(req_serve_time[0]) + float(req_serve_time[1]) + float(req_serve_time[2])) * 1000000
                                 ) + ' '

        req_protocol = line_split[14]
        main_req_protocol = req_protocol[0:] + ' '

        req_method = line_split[12]
        formatted_req_method = req_method[1:] + ' '
        req_url = line_split[13]
        formatted_req_url = req_url[37:]
        main_req_line = '"' + formatted_req_method + formatted_req_url + ' '

        main_status_code = line_split[8] + ' '

        main_referer = '"-"'

        main_recived_bytes = line_split[10] + ' '

        main_sent_bytes = line_split[11] + ' '

        user_agent = line_split[15:-15]
        main_user_agent = ''
        for j in user_agent:
            main_user_agent += ' ' + j

        main_line = (
                    main_date_time + 
                    main_session_id + 
                    main_client_ip + 
                    main_resp_bytes + 
                    main_req_serve_time + 
                    main_req_line + 
                    main_req_protocol + 
                    main_status_code + 
                    main_referer + 
                    main_recived_bytes + 
                    main_sent_bytes + 
                    main_user_agent
                    )

        with open(apache_log_file, 'a') as apache:
            apache.write(main_line + '\n')

    print(file + ' file prcessed succesfully.')


end_time = time.time()
total_time = '{0:.3f}'.format(end_time - start_time)

print('''all logs processed and saved in {} file.
Total time taken to process the log files is {} sec.'''.format(apache_log_file, total_time))
