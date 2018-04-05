#!/usr/bin/python3
import csv
from collections import Counter
from nltk import ngrams
import datetime


def csv_reader(filename): 
    
    with open(filename,'r') as file_obj:
    	data_list = list(csv.reader(file_obj))
    	return data_list


def search_for_users_by_the_number_of_requests(data_list):
	
	user_list = [data_list[i][1] for i in range(1, len(data_list)) if data_list[i][1] != '']
	user_list = Counter(user_list).most_common(5)

	result_file.write('# Отчет\n\n# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов. \n\n')
	for i in range(len(user_list)):
		result_file.write('Пользователь: {}     Количество запросов: {}\n'.format(user_list[i][0], user_list[i][1]))
	result_file.write('\n\n')


def search_for_users_by_the_amount_of_data(data_list):

	user_input_dict = {}
	#user_input_dict key[src_user] = input_byte
	for i in range(1, len(data_list)):
		if data_list[i][1] == '': continue
		if data_list[i][1] not in user_input_dict:
			user_input_dict[data_list[i][1]] = int(data_list[i][7])
		else: 
			user_input_dict[data_list[i][1]] +=int(data_list[i][7])

	user_input_dict = Counter(user_input_dict).most_common(5)

	result_file.write('# Поиск 5ти пользователей, отправивших наибольшее количество данных. \n\n')
	for i in range(len(user_input_dict)):
		result_file.write('Пользователь: {}     Количество переданных данных: {}\n'.format(user_input_dict[i][0], user_input_dict[i][1]))
	result_file.write('\n\n')


def search_for_regular_requests(data_list, task):

	if task == 3: 
		f, field  = 1, 'src_user'
	if task == 4: 
		f, field  = 2, 'src_ip'

	list_request = []
	#list_request is a list of lists [request, time]
	count_result, interval_result = dict(), dict()
	#count_result[request] = interval counter
	#interval_result [request] = [current time, current interval]
	for i in range(1, len(data_list)):
		if data_list[i][1] != '':
			request = data_list[i][f] + ' ' + data_list[i][3] + ' '+ data_list[i][5] + ' ' + data_list[i][6]
			list_request.append([request, data_list[i][0]])
		
	for i in range(len(list_request)):
		key = list_request[i][0]
		time = list_request[i][1]

		if count_result.get(key) == None:
			count_result[key] = 0
			interval_result[key] = [time, 0]
		else: 
			new_time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.000+0300")
			last_time = datetime.datetime.strptime(interval_result[key][0], "%Y-%m-%dT%H:%M:%S.000+0300")

			if last_time > new_time:
				interval = last_time - new_time 
			else:
				interval = new_time - last_time 

			interval_result[key][0] = time

			if interval_result[key][1] == interval.seconds:
				count_result[key] += 1 
			else: 
				interval_result[key][1] = interval.seconds

	result_file.write('# Поиск регулярных запросов по полю {}.\n# Реализован поиск идущих подряд с равными интервалами запросов вида: [{}, src_port, dest_ip, dest_port].\n\n'.format(field, field))
	count_result = Counter(count_result).most_common(5)
	for i in range(0, 5):
		result_file.write('Запрос: {}\nДлина интервала в секундах: {}\nКоличество интервалов: {}\n\n'.format(count_result[i][0], interval_result[count_result[i][0]][1], count_result[i][1]))
	result_file.write('\n')


def search_Ngrams(data_list):

	data_set = []
	#data_set is a list of lists [src_user + src_port + dest_ip + dest_port]
	for i in range(1, len(data_list)):
		if data_list[i][1] != '':
			data_set.append(data_list[i][1] + data_list[i][3] + data_list[i][5] + data_list[i][6])
	
	result_file.write('# Рассматривая события сетевого трафика как символы неизвестного языка,\n# найти 5 наиболее устойчивых N-грамм журнала событий.\n\n')
	for n in range(3, 6):
		result_file.write('Для N = {}\n\n'.format(n))

		most_sustainable_ngrams = Counter(list(ngrams(data_set, n))).most_common(5)

		for i in range(len(most_sustainable_ngrams)):
			result_file.write('Количество вхождений N-граммы: {}\nN-грамма:\n{}\n\n'.format(most_sustainable_ngrams[i][1], most_sustainable_ngrams[i][0]))


if __name__ == '__main__':

	data_list = csv_reader('shkib.csv')
	result_file = open('result.txt', 'a', encoding='utf-8')

	search_for_users_by_the_number_of_requests(data_list)
	search_for_users_by_the_amount_of_data(data_list)
	search_for_regular_requests(data_list, 3)
	search_for_regular_requests(data_list, 4)
	search_Ngrams(data_list)

	result_file.close()

