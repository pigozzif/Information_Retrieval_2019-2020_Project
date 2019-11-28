import threading
import requests
from requests.exceptions import RequestException, Timeout
from bs4 import BeautifulSoup

import frontier
import web_crawler


class SingleCrawler(threading.Thread):
    """
    Implementation of a single thread within a pool of workers. It inherits
    from threading.Thread, extending the functionalities of a thread object.
    This class is in charge of performing all those actions that are target
    of a specific thread/process (managing the frontier, ...), as well as
    storing thread-local data (like the frontier).
    """

    def __init__(self, name, mother, num_urls_to_crawl, seed, user_agent,
                 num_back_queues, num_front_queues, prioritizer):
        """
        Create a SingleCrawler instance
        :param name: the id of the thread in the pool, as far as we are concerned
        :param mother: a reference to the WebCrawler class
        :param num_urls_to_crawl: an int for total number of iterations to run
        :param seed: the webpage to start from
        :param user_agent: the user-agent of the bot
        :param num_back_queues: delegated to the Frontier constructor
        :param num_front_queues: delegated to the Frontier constructor
        :param prioritizer: a function that, given a webpage, assigns a priority score
        in [0, 1]
        """
        threading.Thread.__init__(self)
        self.setName(name)
        self.__num_urls_to_crawl = num_urls_to_crawl
        self.__mother = mother
        self.__fetch_times = dict()  # a dictionary of (host: last-fetch-time) pairs
        self.__fetch_times[web_crawler.WebCrawler.resolve_hostname(seed)[0]] = 0  # we assume no delay for the seed
        self.__frontier = frontier.Frontier(num_back_queues, num_front_queues)
        self.__frontier.enqueue(seed, 1, self.__fetch_times)
        self.__user_agent = user_agent
        self.__prioritizer = prioritizer
        self.__count = 0

    def get_length_frontier(self):
        """
        Retrieve number of urls sitting in the frontier. Needed from outside to
        compute statistics.
        :return: the length of the frontier
        """
        return len(self.__frontier)

    def get_count_urls_crawled(self):
        """
        Retrieve number of urls crawled. Needed from outside to
        compute statistics.
        :return: the number of urls crawled
        """
        return self.__count

    def get_num_hosts_contacted(self):
        """
        Retrieve the number of unique hosts contacted during the crawl.
        :return: the length of the fetch times dictionary
        """
        return len(self.__fetch_times)

    def run(self):
        """
        Run the main crawler loop.
        """
        # iterate as long as we do not reach the target number of pages to crawl
        while self.__count < self.__num_urls_to_crawl:
            # dequeue an url from the frontier
            curr_url = self.__frontier.dequeue(self.__fetch_times)
            # print(curr_url)
            # before proceeding, make sure the url is allowed from the robots.txt. As suggested
            # by the book (page 447), the robots.txt should be checked just before fetching, because
            # on large-scale crawls a url might reside on the frontier for days; in the meanwhile, the
            # file might have changed. Of course, here it makes sense to trade performance for conservatism
            rules = web_crawler.WebCrawler.be_fair(curr_url, self.__user_agent)
            if not rules["allowed"]:
                continue

            # fetch the webpage as a requests.models.Response object
            response = self.__fetch(curr_url)
            if response is None:
                continue
            self.__count += 1
            # update the last fetch time for the host
            self.__fetch_times[web_crawler.WebCrawler.resolve_hostname(curr_url)[0]] = response.elapsed.total_seconds()
            # parse the webpage for robust urls and assign them a priority score
            parsed_urls = self.__parse_urls(response)
            priorities = [self.__prioritizer(url) for url in parsed_urls]  # beware it should not be the url
            # recall the delay specified in the robots.txt, if any. Otherwise, we follow again the suggestion
            # of the book (page 453), we define the last fetch time times ten as desired delay
            self.__fetch_times[web_crawler.WebCrawler.resolve_hostname(curr_url)[0]] = rules["delay"] if rules["delay"]\
                else response.elapsed.total_seconds()

            # SYNCHRONIZED PART
            with threading.Lock():  # acquire a lock
                # sift out duplicates and update the url table
                parsed_urls = self.__mother.check_and_update_urls(parsed_urls)
                # ask the mother to assign the urls to the appropriate thread
                self.__mother.synchronize_frontiers(parsed_urls, self.getName(), priorities)
                # update the global array of webpage responses
                self.__mother.retrieve(response)

    def __fetch(self, url, time_quantum=1.0, num_attempt=0):
        """
        Fetch a webpage from the corresponding url. If a page does not answer,
        try for a fixed number of attempts with an increasing timed wait. We follow
        the values explained in the book (page 450) when referring to the Mercator
        crawler.
        :param url: a string for the url under discussion
        :param time_quantum: timeout for the request
        :param num_attempt: how many times in row we have tried
        to retrieve the page
        :return: a requests.models.Response object, None for failure
        """
        try:
            response = requests.get(url, headers={"user-agent": self.__user_agent}, timeout=time_quantum)
        except Timeout:  # retry for a maximum of 4 times, each time trice the time quantum
            if num_attempt >= 4:
                return None
            return self.__fetch(url, time_quantum=time_quantum * 3, num_attempt=num_attempt + 1)
        except RequestException:  # parent class for all the other errors that can occur
            return None

        # filter out those whose status code is not a 200 (OK). This cuts out also the tricky
        # 414s, an example of a crawler trap
        if response.status_code != 200:
            return None
        return response

    def __parse_urls(self, response):
        """
        Extract all the urls contained in a page using BeautifulSoup, and return the
        robust ones.
        :param response: a requests.models.Response object
        :return: a list for the valid urls
        """
        source_url = response.url
        soup = BeautifulSoup(response.text, "html.parser")
        # as a matter of example: "en.wikipedia.org" is strip_base, "https://en.wikipedia.org" is base_url,
        # "https://en.wikipedia.org/wiki/Main_Page" is path
        strip_base, base_url, path = web_crawler.WebCrawler.resolve_hostname(source_url)
        parsed_urls = set()

        # iterate over all the HTML anchor tags
        for link in soup.find_all("a"):
            # check if they have an href attribute
            anchor = link.get("href")
            if anchor is None:
                continue

            # deal with different formatting styles for href urls
            curr_url = anchor
            if anchor.startswith('/'):
                curr_url = base_url + anchor
            elif strip_base in anchor:
                curr_url = anchor
            elif not anchor.startswith('http'):
                curr_url = path + anchor

            # test the url for robustness
            curr_url, status = web_crawler.WebCrawler.be_robust(curr_url)
            if status:
                parsed_urls.add(curr_url)
            # else:
            #     print("here is a faulty one: " + curr_url)
        return list(parsed_urls)

    def frontier_add(self, url, priority):
        """
        Interface for adding a url to the frontier owned by the thread.
        :param url: a string for the url to enqueue
        :param priority: its priority score, in [0, 1]
        """
        self.__frontier.enqueue(url, priority, self.__fetch_times)

    def frontier_remove(self):
        """
        Interface for popping a url from the frontier owned by the thread.
        :return: the dequeued url
        """
        return self.__frontier.dequeue(self.__fetch_times)
