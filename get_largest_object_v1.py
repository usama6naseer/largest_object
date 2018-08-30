from bs4 import BeautifulSoup
import requests
import csv
import multiprocessing
import sys

def is_object_under_same_domain(obj_link, url):
	if obj_link is not None and len(obj_link) > 1:
		if obj_link[0] == '/' and obj_link[1] == '/':
			return 'https:' + obj_link

		elif obj_link[0] == '/':
			if url[len(url) - 1] == '/':
				return url[:-1] + obj_link
			else:
				return url + obj_link
		else:
			if url in obj_link:
				return obj_link
			else:
				return False
	else:
		return False

REDIRECTION_MAP = {}

def get_largest_obj(url, count, meta):
	global REDIRECTION_MAP

	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
	res  = requests.get(url, headers=headers, allow_redirects=False, timeout=10)
	res_code = res.status_code
	# print(url, res_code)

	if res_code == 301 or res_code == 302:
		if 'Location' in res.headers:
			new_url = res.headers['Location']
			REDIRECTION_MAP[meta[1]] = new_url

		REDIRECTION_MAP[meta[1]] = new_url
		cw = csv.writer(open('site-codes.csv', 'a'))
		cw.writerow([meta[0], meta[1], url, count, res_code, new_url])
		get_largest_obj(new_url, count, meta)

	elif res_code == 200:
		cw = csv.writer(open('site-codes.csv', 'a'))
		cw.writerow([meta[0], meta[1], url, count, res_code, ''])

		data = res.text
		soup = BeautifulSoup(data, "html.parser")
		max_obj_size = 0
		max_obj_url = ''
		cn = 0

		for link in soup.find_all('a'):
			cn += 1
			if cn > 100:
				break

			try:
				obj_link = link.get('href')
				if obj_link is not None:
					obj_url = is_object_under_same_domain(obj_link, url)
					# print(1, obj_link, obj_url)

					if obj_url != False:

						continue_flag = True
						continuations = 0
						while continue_flag == True:
							continuations += 1
							if continuations > 5:
								continue_flag = False

							obj_res  = requests.get(obj_url, allow_redirects=False, timeout=10)

							res_code = obj_res.status_code
							# print(obj_url, res_code)

							if res_code == 301 or res_code == 302:
								if 'Location' in obj_res.headers:
									obj_url = obj_res.headers['Location']
									# print('new:', obj_url)

							elif res_code == 200:
								continue_flag = False

								obj_size = len(obj_res.content)
								# print(obj_url, len(obj_res.content), obj_res.headers)
								# print(obj_res.status_code)
								# print(obj_url, len(obj_res.content), obj_res.status_code)

								if obj_size > max_obj_size:
									max_obj_size = obj_size
									max_obj_url = obj_url

								if count == 1:
									temp = get_largest_obj(obj_url, count - 1, meta)
									if temp[1] > max_obj_size:
										max_obj_url = temp[0]
										max_obj_size = temp[1]
							else:
								continue_flag = False

			except Exception as e:
				print(101, e)
				pass

		# print(max_obj_url, max_obj_size)
		return [max_obj_url, max_obj_size, cn]

	else:
		return ['', 0, 0]


def worker(start_point, end_point):
	cn = 0
	cr = csv.reader(open('top-1m.csv', 'r'))
	for row in cr:
		# cn += 1
		rank = row[0]
		cn = int(rank)
		# print(row)
		# if int(rank) >= 11:
		# if int(rank) < end_point:
		if cn > start_point and cn <= end_point:
			url = row[1]
			try:
				# url = 'siteadvisor.com'
				print('testing:', url, rank, cn)
				ret = get_largest_obj('https://www.' + url, 0, [rank, url])
				print('ret:', ret)

				cw = csv.writer(open('largest_obj.csv', 'a'))
				cw.writerow([rank, url, ret[0], ret[1], ret[2], cn])

			except Exception as e:
				print(e)
				cw_log = csv.writer(open('log.csv', 'a'))
				cw_log.writerow([rank, url])
				pass


if __name__ == "__main__":

	# p = multiprocessing.Process(target=worker, args=(0, 1,))
	# worker(0, 10001)

	jobs = []
	start_point = 0
	total = 100000
	while True:
		if start_point > total:
			break	
		
		end_point = start_point + total/10

		p = multiprocessing.Process(target=worker, args=(start_point, end_point,))
		jobs.append(p)
		p.start()

		start_point = end_point










