from bs4 import BeautifulSoup
import requests
import csv
import multiprocessing

def is_object_under_same_domain(obj_link, url):
	if obj_link is not None and len(obj_link) > 0:
		if obj_link[0] == '/':
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

def get_largest_obj(url, count):
	# print('testing:', url, count)

	res  = requests.get(url)
	# print(res.text)
	data = res.text
	soup = BeautifulSoup(data, "html.parser")
	max_obj_size = 0
	max_obj_url = ''
	cn = 0

	for link in soup.find_all('a'):
		cn += 1
		if cn > 100:
			break

		obj_link = link.get('href')
		obj_url = is_object_under_same_domain(obj_link, url)
		# print(1, obj_link, obj_url)

		if obj_url != False:
			obj_res  = requests.get(obj_url)
			obj_size = len(obj_res.content)
			# print(obj_url, len(obj_res.content), obj_res.headers)
			# print(obj_url, len(obj_res.content))

			if obj_size > max_obj_size:
				max_obj_size = obj_size
				max_obj_url = obj_url

			if count == 1:
				temp = get_largest_obj(obj_url, count - 1)
				if temp[1] > max_obj_size:
					max_obj_url = temp[0]
					max_obj_size = temp[1]

	# print(max_obj_url, max_obj_size)
	return [max_obj_url, max_obj_size, cn]

def worker(start_point, end_point):
	cn = 0
	cr = csv.reader(open('top-1m.csv', 'r'))
	for row in cr:
		cn += 1
		if cn >= start_point and cn <= end_point:
			rank = row[0]
			url = row[1]
			try:
				# url = 'siteadvisor.com'
				print('testing:', url, rank)
				ret = get_largest_obj('http://www.' + url, 0)
				print('ret:', ret)

				cw = csv.writer(open('largest_obj.csv', 'a'))
				cw.writerow([rank, url, ret[0], ret[1], ret[2], cn])

			except Exception as e:
				print(e)
				cw_log = csv.writer(open('log.csv', 'a'))
				cw_log.writerow([rank, url])
				pass


if __name__ == "__main__":

	# jobs = []
	# start_point = 100000
	# for i in range(0,90):
	# 	end_point = start_point + 10000

	# 	p = multiprocessing.Process(target=worker, args=(start_point, end_point,))
	# 	jobs.append(p)
	# 	p.start()

	# 	start_point = end_point

	jobs = []
	start_point = 0
	total = 10
	while True:
		if start_point > total:
			break	
		
		end_point = start_point + total/10

		p = multiprocessing.Process(target=worker, args=(start_point, end_point,))
		jobs.append(p)
		p.start()

		start_point = end_point










