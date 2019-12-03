#
# THIS IS A SIMPLE MAIN SCRIPT THAT IS SUPPOSED TO BE EDITED BY THE USER
#

import web_crawler


# define some custom variables, the user should edit them most likely
my_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko)' \
                ' Chrome/78.0.3904.97 Safari/537.36'
seeds = {0: ["https://en.wikipedia.org/wiki/Main_Page"], 1: ["https://it.wikipedia.org/wiki/Pagina_principale"]}

# create a WebCrawler instance and generate webpages (as requests.models.Response objects)
our_crawler = web_crawler.WebCrawler()
for webpage in our_crawler.crawl(100, seeds, my_user_agent, num_threads=1):

    # HERE THE USER CAN DO ANY DESIRED MANIPULATION ON THE YIELDED
    # PAGE. FOR THE SAKE OF ILLUSTRATION, WE ARE SIMPLY PRINTING ITS
    # URL, BUT WE COULD ALSO DO SOMETHING WITH webpage.content, or
    # webpage.text, ...
    print(webpage.url)
