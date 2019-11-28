import re
import random
import time
import threading
import logging
from requests.exceptions import RequestException
from urllib.parse import urlparse, urlunparse, urlsplit
from reppy.robots import Robots
from reppy.exceptions import ReppyException

import single_crawler


class WebCrawler(object):
    """
    Implementation of a web crawler. In particular, this
    class is capable of spawning a team of workers, and generate
    crawled pages as they progress. While each thread is represented
    by a distinct class (SingleCrawler), WebCrawler is responsible
    for storing global or shared data (like the table for unique urls,
    ...), as well as incapsulating functionalities that require some
    synchronization among threads, or that are static in nature (like
    checking if a url is fair/robust).
    """

    def __init__(self):
        """
        Create an instance of a web crawler.
        """
        self.__urls = set()  # a hashtable for the uniques urls found
        self.__host_to_thread = dict()  # a dicionary of (host, thread) pairs
        self.__threads = list()  # to store the Thread objects
        self.__output = list()  # to store the output pages for the user

    @staticmethod
    def resolve_hostname(url):
        """
        Static method that, given a url, retrieves some of its building blocks.
        :param url: a string for the url under discussion
        :return: a tuple with the bare domain, the protocol + domain, and the full path
        """
        parts = urlsplit(url)
        base = "{0.netloc}".format(parts)
        # example: en.wikipedia.org
        strip_base = base.replace("www.", "")
        # example: https://en.wikipedia.org
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        # example: https://en.wikipedia.org/wiki/Main_Page
        path = url[:url.rfind('/') + 1] if '/' in parts.path else url
        return strip_base, base_url, path

    @staticmethod
    def be_fair(url, user_agent):
        """
        Static method to test whether a url satisfies the robots.txt for its
        hostname. It also returns the delay specified for the user-agent, if any.
        :param url: a string for the url under discussion
        :param user_agent: the user agent performing the request
        :return: a dictionary with a boolean value for "allowed" key and a timedelta
        value for "delay" key
        """
        # retrieve the url of the robots.txt, try to fetch it
        robots_url = Robots.robots_url(url)
        try:
            robots = Robots.fetch(robots_url, headers={"user-agent": user_agent})
        except (ReppyException, RequestException):
            return {"allowed": False, "delay": None}
        return {"allowed": robots.allowed(url, user_agent), "delay": robots.agent(user_agent).delay}

    @staticmethod
    def be_robust(url):
        """
        Static method to test whether a url satisfies some robustness heuristics.
        :param url: the url to test against
        :return: a pair with the simplified url and a boolean denoting yes/no robustness
        """
        # the traps and heuristics hereon are inspired from:
        # https://www.searchenginejournal.com/crawler-traps-causes-solutions-prevention/305781/
        # https://support.archive-it.org/hc/en-us/articles/208332963-How-to-modify-your-crawl-scope-with-a-Regular-Expression
        # as far as the user is concerned, fragments in web urls provide no information, so we remove them.
        # Moreover, we also get rid of the query since it aliases with faceted filtering, a horrible crawler trap
        url = urlparse(url)
        url = urlunparse((url.scheme, url.netloc, url.path, url.params, "", ""))

        # urls that are too long, since they are probably tricky (2048 is the value used by Googlebot)
        if len(url) > 2048:
            return url, False
        # urls that have hundreds of subdirectories
        elif re.search(r"^.*/[^/]{300,}$", url) is not None:
            return url, False
        # urls that have the same directory repeating different times
        elif re.search(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", url) is not None:
            return url, False
        # urls that have some common directories repeating more than three times
        elif re.search(r"^.*(/misc|/sites|/all|/themes|/modules|/profiles|/css|"
                       r"/field|/node|/theme){3,}.*$", url) is not None:
            return url, False
        # urls that have the word 'calendar' in them, sometimes related to a fatal crawler trap
        elif re.search(r"^.*calendar.*$", url) is not None:
            return url, False
        return url, True

    def check_and_update_urls(self, urls):
        """
        Filter duplicates from a list of urls, using a table common to all
        threads.
        :param urls: a list for the urls under discussion
        :return: the parsed list
        """
        result = list(filter(lambda x: x not in self.__urls, urls))
        self.__urls.update(urls)
        return result

    def synchronize_frontiers(self, urls, sender, priorities):
        """
        Given a list of urls, for each one resolve which thread "owns" its domain
        and add to the corresponding frontier.
        :param urls: a list for the urls under discussion
        :param sender: the name of the calling thread
        :param priorities: a list for the priority scores of the urls
        """
        # iterate over the urls
        for num in range(len(urls)):
            url = urls[num]
            # retrieve the thread responsible for that domain
            hostname, _, _ = self.resolve_hostname(url)
            owner = self.__host_to_thread.get(hostname, None)

            # none has already reached the host. Then, it is assigned to
            # the calling thread
            if owner is None:
                self.__host_to_thread[hostname] = sender
                owner = sender

            # retrieve the Thread object and delegate enqueuing to its frontier
            for thread in threading.enumerate():
                if thread.getName() == owner:
                    thread.frontier_add(url, priorities[num])

    def retrieve(self, out_page):
        """
        Append a response to the output list, ready to be delivered to the
        user.
        :param out_page: a requests.models.Response object
        """
        self.__output.append(out_page)

    def log_info(self):
        """
        Collect and log to the stderr information about number of active
        (non-main) threads, total number of urls crawled, aggregate frontier
        size and number of hosts contacted. Supposed to be run by the main
        thread in a typical workflow.
        """
        frontier_size = sum([thread.get_length_frontier() for thread in self.__threads])
        num_urls = sum([thread.get_count_urls_crawled for thread in self.__threads])
        num_hosts = sum([thread.get_num_hosts_contacted() for thread in self.__threads])
        logging.info("\nn° active non-main threads: " + str(threading.active_count() - 1) +
                     "\nn° unique urls crawled: " + str(num_urls) +
                     "\naggregate frontier size: " + str(frontier_size) +
                     "\nn° hosts contacted: " + str(num_hosts) + "\n")

    def crawl(self, num_urls, seeds, user_agent, num_threads, prioritizer=lambda x: random.random(), verbose=1):
        """
        Run the overall crawling pipeline. In particular, different threads are instantiated
        and instructed to progress the crawling. In the meanwhile, the main thread (that is
        sitting idle) "listens" for changes to self.__output (where fetched pages are dumped
        by individual threads) and yields them to the user. This function is supposed to
        be used as a generator.
        :param num_urls: total number of urls to crawl
        :param seeds: the entry points for the threads
        :param user_agent: the executing user-agent
        :param prioritizer: a function that, given a url, assigns a priority score in [0, 1]
        :param num_threads: number of threads to spawn
        :param verbose: whether to log information every 30 seconds
        :return: generates requests.models.Response objects for the fetched pages
        """
        # perform some checks on user-provided input
        assert len(seeds) == num_threads, "less seeds than threads"
        assert len(set(seeds)) == len(seeds), "using duplicate seeds results in empty frontier heaps"

        # format logging and distribute urls among threads
        form = "%(asctime)s: %(message)s"
        logging.basicConfig(format=form, level=logging.INFO, datefmt="%H:%M:%S")
        num_urls_per_thread = int(num_urls / num_threads)

        # create thread instances and launch them; they take care of the crawling. Notice that, following what
        # the designers of Mercator suggest according to the book (page 453), we delegate the creation of three times
        # as many back queues (for each frontier) as crawler threads. The number of front queues is just a placeholder
        # FORK THE TEAM
        for name in range(num_threads):
            self.__threads.append(single_crawler.SingleCrawler(str(name), self, num_urls_per_thread, seeds[name],
                                                               user_agent, num_threads * 3, 5, prioritizer))
            self.__threads[-1].start()

        # this block is executed by the main thread. Notice this is a VERY rough implementation
        # of the Observer Pattern
        if threading.current_thread().getName() == "MainThread":
            current_pos = 0
            prec_time = time.time()
            # loop until there are no active threads working in the team
            while threading.active_count() > 1:
                # check for any change in the array storing the output pages. If a new one
                # has been appended, yield it to the user
                if len(self.__output) > current_pos:
                    yield self.__output[-1]
                    current_pos += 1
                # if required, log useful info every 30 seconds
                if verbose and time.time() >= prec_time + 30:
                    self.log_info()
                    prec_time = time.time()

        # JOIN THE TEAM
        for thread in self.__threads:
            thread.join()
