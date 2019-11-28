import random
import math
import time
import queue
import heapq

import web_crawler


class Frontier(object):
    """
    Implementation of a frontier, the piece of a crawler
    architecture devoted to storing discovered urls, and producing
    them as a function of priority (to ensure quality) and time
    from last fetch (to ensure fairness). Here we follow the architecture
    proposed in lecture, with the missing part of the freshness score, since
    this was the focus for the other project.
    """

    def __init__(self, num_back_queues, num_front_queues):
        """
        Create a Frontier instance.
        :param num_back_queues: number of back queues
        :param num_front_queues: number of front queues
        """
        self.__size = 0
        # Priority is assumed to be a score in [0, 1] assigned to a given url. We wish to evenly partition
        # this domain among the front queues.
        self.__interval = int(100 * (1 / num_front_queues))
        # a dictionary mapping the upper bound of every interval to the corresponding Queue object
        self.__front_queues = {(end + self.__interval) / 100: queue.Queue(maxsize=0) for end
                               in range(0, 100, self.__interval)}
        self.__interval /= 100  # before we needed integers for computing range()

        self.__num_back_queues = num_back_queues
        self.__back_queues = dict()  # will become a dictionary of host: Queue pairs

        self.__heap = list()
        heapq.heapify(self.__heap)
        # define the array of probabilities to be used by the prioritizer, such that each front queue
        # in order of priority has twice the likelihood of the preceding one to be chosen
        curr_sum = 1
        self.__probabilities = list()
        for num in range(num_front_queues):
            self.__probabilities.append(curr_sum + num)
            curr_sum += num

    def __len__(self):
        """
        Compute the 'length' of the frontier as the number of urls
        that have been enqueued and not yet dequeued.
        :return: the aforementioned number
        """
        return self.__size

    def enqueue(self, url, priority, fetch_times):
        """
        Add an url to the frontier.
        :param url: a string for the url to add
        :param priority: a float in [0, 1] for the priority of the url
        :param fetch_times: a dictionary of (host: last-fetch-time) pairs
        the host for the url
        """
        # if some of the back queues have yet to be assigned a url, push the current url
        # down into one of them. Otherwise, they would never be used
        if len(self.__back_queues) < self.__num_back_queues:
            host, _, _ = web_crawler.WebCrawler.resolve_hostname(url)

            if host not in self.__back_queues:
                q = queue.Queue(maxsize=0)
                self.__back_queues[host] = q
            else:
                q = self.__back_queues[host]

            q.put(url)
            # push into the heap a pair for the earliest time to contact again the host
            # following what suggested by the book (page 453), the new heap entry will have the current time
            # plus ten times the last fetch time (which is actually an average across all the fetch times)
            delay = fetch_times.get(host, sum(fetch_times.values()) / len(fetch_times))
            self.__heap_replace(time.time() + delay * 10, host)
        else:  # else, simply assign the url using the prioritizer
            self.__front_route(url, priority)

        self.__size += 1

    def dequeue(self, fetch_times):
        """
        Pop an element from the frontier and return it to the caller.
        :param fetch_times: a dictionary of (host: last-fetch-time) pairs
        :return: a string for the popped url
        """
        if self.__size == 0:
            raise IndexError("Heap is Empty. Troubles fetching urls?")

        wait_time, host = heapq.heappop(self.__heap)
        q = self.__back_queues[host]
        result_url = q.get()

        # if the queue is empty, we need to refill from the front queues in a biased manner
        if q.empty():
            host = self.__back_route(q, host)

        if host is not None:  # self.__back_route returns None if there are no urls left in the front queues
            # wait the required time before contacting the host again
            if time.time() < wait_time:
                # we take the maximum because the above difference could be so small that it is elapsed
                # before we call .sleep(), leading to a negative value and an exception
                time.sleep(max(0, wait_time - time.time()))
            # following what suggested by the book (page 453), the new heap entry will have the current time
            # plus ten times the last fetch time (which is actually an average across all the fetch times)
            delay = fetch_times.get(host, sum(fetch_times.values()) / len(fetch_times))
            self.__heap_replace(time.time() + delay * 10, host)

        self.__size -= 1
        return result_url

    def __back_route(self, q, our_host):
        """
        Apply the back routing algorithm for picking an url from the front queues to be
        assigned to the back queues
        :param q: the back Queue object
        :param our_host: the host the queue was dedicated before
        :return: the new host for the queue (can be the same as our_host)
        """
        # iterate as long as there are urls left in the front queues, unless we break because
        # we have found a new host for the queue
        while not all([front_queue.empty() for front_queue in self.__front_queues.values()]):
            # make a biased selection among the front queues according to the probabilities defined in the
            # constructor
            selected_front = random.choices(list(self.__front_queues.values()), weights=self.__probabilities)[0]
            candidate_url = selected_front.get()
            candidate_host, _, _ = web_crawler.WebCrawler.resolve_hostname(candidate_url)

            # three cases: an url of the same host, an url belonging to the host of another queue, or a url of a fresh
            # new host we can use
            if candidate_host == our_host:
                q.put(candidate_url)
                break
            elif candidate_host in self.__back_queues:
                self.__back_queues[candidate_host].put(candidate_url)
            else:
                del self.__back_queues[our_host]
                self.__back_queues[candidate_host] = q
                q.put(candidate_url)
                our_host = candidate_host
                break
        else:
            return None

        return our_host

    def __front_route(self, url, priority):
        """
        Assign a url to a front queue according to its priority score.
        :param url: a string for the url to add
        :param priority: a float in [0, 1] for the priority of the url
        """
        prior = math.floor(priority / self.__interval)  # the index of the chosen front queue
        front_q = list(self.__front_queues.keys())[prior]  # the key corresponding to the index
        self.__front_queues[front_q].put(url)

    def __heap_replace(self, new_time, host):
        """
        Wrapper around heapq.heappush which updates the time if host
        is already present.
        :param new_time: the time to wait before fetching again
        :param host: the hostname under discussion
        """
        # linearly scan the heap and look for duplicates
        for idx in range(len(self.__heap)):
            t, h = self.__heap[idx]
            if h == host:
                self.__heap.pop(idx)  # eliminate the duplicate
                break
        # update the heap with the new time
        heapq.heappush(self.__heap, (new_time, host))
