import datetime as dt
import functools
import io
import logging
import multiprocessing.dummy
import time

from homeassistant.components.camera import PLATFORM_SCHEMA, Camera
from homeassistant.helpers import config_validation as cv
from PIL import Image
from voluptuous import All, In, Optional, Required
import requests

_LOGGER = logging.getLogger(__name__)

CONF_LOC = 'location'
CONF_NAME = 'name'

radars = {
    'Adelaide':        {'id': '643', 'delta': 360, 'frames': 6},
    'Albany':          {'id': '313', 'delta': 600, 'frames': 4},
    'AliceSprings':    {'id': '253', 'delta': 600, 'frames': 4},
    'Bairnsdale':      {'id': '683', 'delta': 600, 'frames': 4},
    'Bowen':           {'id': '243', 'delta': 600, 'frames': 4},
    'Brisbane':        {'id': '663', 'delta': 360, 'frames': 6},
    'Broome':          {'id': '173', 'delta': 600, 'frames': 4},
    'Cairns':          {'id': '193', 'delta': 360, 'frames': 6},
    'Canberra':        {'id': '403', 'delta': 360, 'frames': 6},
    'Carnarvon':       {'id': '053', 'delta': 600, 'frames': 4},
    'Ceduna':          {'id': '333', 'delta': 600, 'frames': 4},
    'Dampier':         {'id': '153', 'delta': 600, 'frames': 4},
    'Darwin':          {'id': '633', 'delta': 360, 'frames': 6},
    'Emerald':         {'id': '723', 'delta': 600, 'frames': 4},
    'Esperance':       {'id': '323', 'delta': 600, 'frames': 4},
    'Geraldton':       {'id': '063', 'delta': 600, 'frames': 4},
    'Giles':           {'id': '443', 'delta': 600, 'frames': 4},
    'Gladstone':       {'id': '233', 'delta': 600, 'frames': 4},
    'Gove':            {'id': '093', 'delta': 600, 'frames': 4},
    'Grafton':         {'id': '283', 'delta': 600, 'frames': 4},
    'Gympie':          {'id': '083', 'delta': 360, 'frames': 6},
    'HallsCreek':      {'id': '393', 'delta': 600, 'frames': 4},
    'Hobart':          {'id': '763', 'delta': 360, 'frames': 6},
    'Kalgoorlie':      {'id': '483', 'delta': 360, 'frames': 6},
    'Katherine':       {'id': '423', 'delta': 360, 'frames': 6},
    'Learmonth':       {'id': '293', 'delta': 600, 'frames': 4},
    'Longreach':       {'id': '563', 'delta': 600, 'frames': 4},
    'Mackay':          {'id': '223', 'delta': 600, 'frames': 4},
    'Marburg':         {'id': '503', 'delta': 600, 'frames': 4},
    'Melbourne':       {'id': '023', 'delta': 360, 'frames': 6},
    'Mildura':         {'id': '303', 'delta': 600, 'frames': 4},
    'Moree':           {'id': '533', 'delta': 600, 'frames': 4},
    'MorningtonIs':    {'id': '363', 'delta': 600, 'frames': 4},
    'MountIsa':        {'id': '753', 'delta': 360, 'frames': 6},
    'MtGambier':       {'id': '143', 'delta': 600, 'frames': 4},
    'Namoi':           {'id': '693', 'delta': 600, 'frames': 4},
    'Newcastle':       {'id': '043', 'delta': 360, 'frames': 6},
    'Newdegate':       {'id': '383', 'delta': 360, 'frames': 6},
    'NorfolkIs':       {'id': '623', 'delta': 600, 'frames': 4},
    'NWTasmania':      {'id': '523', 'delta': 360, 'frames': 6},
    'Perth':           {'id': '703', 'delta': 360, 'frames': 6},
    'PortHedland':     {'id': '163', 'delta': 600, 'frames': 4},
    'SellicksHill':    {'id': '463', 'delta': 600, 'frames': 4},
    'SouthDoodlakine': {'id': '583', 'delta': 360, 'frames': 6},
    'Sydney':          {'id': '713', 'delta': 360, 'frames': 6},
    'Townsville':      {'id': '733', 'delta': 360, 'frames': 6},
    'WaggaWagga':      {'id': '553', 'delta': 600, 'frames': 4},
    'Warrego':         {'id': '673', 'delta': 600, 'frames': 4},
    'Warruwi':         {'id': '773', 'delta': 360, 'frames': 6},
    'Watheroo':        {'id': '793', 'delta': 360, 'frames': 6},
    'Weipa':           {'id': '783', 'delta': 360, 'frames': 6},
    'WillisIs':        {'id': '413', 'delta': 600, 'frames': 4},
    'Wollongong':      {'id': '033', 'delta': 360, 'frames': 6},
    'Woomera':         {'id': '273', 'delta': 600, 'frames': 4},
    'Wyndham':         {'id': '073', 'delta': 600, 'frames': 4},
    'Yarrawonga':      {'id': '493', 'delta': 360, 'frames': 6},
}

LOCS = sorted(radars.keys())

ERRMSG = "Set 'location' to one of: %s" % ', '.join(LOCS)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    Required(CONF_LOC): All(In(LOCS), msg=ERRMSG),
    Optional(CONF_NAME): cv.string,
})

REQUIREMENTS = ['Pillow==5.4.1']


def log(msg):
    _LOGGER.debug(msg)


def setup_platform(hass, config, add_devices, discovery_info=None):
    location = config.get(CONF_LOC)
    name = config.get(CONF_NAME) or 'BOM Radar Loop - %s' % location
    bomradarloop = BOMRadarLoop(hass, location, name)
    add_devices([bomradarloop])


class BOMRadarLoop(Camera):

    def __init__(self, hass, location, name):
        super().__init__()
        self.hass = hass
        self._location = location
        self._name = name
        self.camera_image()

    def __hash__(self):
        return 1

    def camera_image(self):
        now = int(time.time())
        delta = radars[self._location]['delta']
        start = now - (now % delta)
        return self.get_loop(start)
        
    @functools.lru_cache(maxsize=1)
    def get_background(self, start):

        '''
        Fetch the background map, then the topography, locations (e.g. city
        names), and distance-from-radar range markings, and merge into a single
        image. Cache one image per location, but also consider the 'start'
        value when caching so that bad background images (e.g. with one or more
        missing layers) will be replaced in the next interval.
        '''

        log('Getting background for %s at %s' % (self._location, start))
        radar_id = radars[self._location]['id']
        suffix = 'products/radar_transparencies/IDR%s.background.png'
        url = self.get_url(suffix % radar_id)
        background = self.get_image(url)
        if background is None:
            return None
        for layer in ('topography', 'locations', 'range'):
            log('Getting %s for %s at %s' % (layer, self._location, start))
            suffix = 'products/radar_transparencies/IDR%s.%s.png' % (
                radar_id,
                layer
            )
            url = self.get_url(suffix)
            image = self.get_image(url)
            if image is not None:
                background = Image.alpha_composite(background, image)
        return background

    def get_frames(self, start):

        '''
        Use a thread pool to fetch a set of current radar images in parallel,
        then get a background image for this location, combine it with the
        colorbar legend, and finally composite each radar image onto a copy of
        the combined background/legend image.

        The 'wximages' list is created so that requested images that could not
        be fetched are excluded, so that the set of frames will be a best-
        effort set of whatever was actually available at request time. If the
        list is empty, None is returned; the caller can decide how to handle
        that.
        '''

        log('Getting frames for %s at %s' % (self._location, start))
        fn_get = lambda time_str: self.get_wximg(time_str)
        frames = radars[self._location]['frames']
        pool0 = multiprocessing.dummy.Pool(frames)
        raw = pool0.map(fn_get, self.get_time_strs(start))
        wximages = [x for x in raw if x is not None]
        if not wximages:
            return None
        pool1 = multiprocessing.dummy.Pool(len(wximages))
        background = self.get_background(start)
        if background is None:
            return None
        fn_composite = lambda x: Image.alpha_composite(background, x)
        composites = pool1.map(fn_composite, wximages)
        legend = self.get_legend(start)
        if legend is None:
            return None
        loop_frames = pool1.map(lambda _: legend.copy(), composites)
        fn_paste = lambda x: x[0].paste(x[1], (0, 0))
        pool1.map(fn_paste, zip(loop_frames, composites))
        return loop_frames

    def get_image(self, url):

        '''
        Fetch an image from the BOM.
        '''

        log('Getting image %s' % url)
        response = requests.get(url)
        if response.status_code == 200:
            return Image.open(io.BytesIO(response.content)).convert('RGBA')
        return None

    @functools.lru_cache(maxsize=1)
    def get_legend(self, start):

        '''
        Fetch the BOM colorbar legend image. See comment in get_background()
        in re: caching.
        '''

        log('Getting legend at %s' % start)
        url = self.get_url('products/radar_transparencies/IDR.legend.0.png')
        return self.get_image(url)

    @functools.lru_cache(maxsize=1)
    def get_loop(self, start):

        '''
        Return an animated GIF comprising a set of frames, where each frame
        includes a background, one or more supplemental layers, a colorbar
        legend, and a radar image. See comment in get_background() in re:
        caching.
        '''

        log('Getting loop for %s at %s' % (self._location, start))
        loop = io.BytesIO()
        try:
            frames = self.get_frames(start)
            if frames is None:
                raise
            log('Got %s frames for %s at %s' % (
                len(frames),
                self._location,
                start
            ))
            frames[0].save(
                loop,
                append_images=frames[1:],
                duration=500,
                format='GIF',
                loop=0,
                save_all=True,
            )
        except:
            log('Got NO frames for %s at %s' % (self._location, start))
            Image.new('RGB', (340, 370)).save(loop, format='GIF')
        return loop.getvalue()

    def get_time_strs(self, start):

        '''
        Return a list of strings representing YYYYMMDDHHMM times for the most
        recent set of radar images to be used to create the animated GIF.
        '''

        log('Getting time strings starting at %s' % start)
        delta = radars[self._location]['delta']
        tz = dt.timezone.utc
        mkdt = lambda n: dt.datetime.fromtimestamp(start - (delta * n), tz=tz)
        frames = radars[self._location]['frames']
        return [mkdt(n).strftime('%Y%m%d%H%M') for n in range(frames, 0, -1)]

    def get_url(self, path):

        '''
        Return a canonical URL for a suffix path on the BOM website.
        '''

        log('Getting URL for path %s' % path)
        return 'http://www.bom.gov.au/%s' % path

    @functools.lru_cache(maxsize=max(x['frames'] for x in radars.values()))
    def get_wximg(self, time_str):

        '''
        Return a radar weather image from the BOM website. Note that
        get_image() returns None if the image could not be fetched, so the
        caller must deal with that possibility.
        '''

        log('Getting radar imagery for %s at %s' % (self._location, time_str))
        radar_id = radars[self._location]['id']
        url = self.get_url('/radar/IDR%s.T.%s.png' % (radar_id, time_str))
        return self.get_image(url)

    @property
    def name(self):
        return self._name
