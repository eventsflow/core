''' EventsFlow Core Setup
'''
import site
import setuptools

if __name__ == "__main__":
    # enable user site-package directory
    site.ENABLE_USER_SITE = True
    setuptools.setup()
