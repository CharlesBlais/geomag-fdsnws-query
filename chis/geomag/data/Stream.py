'''
:author: Charles Blais
'''

from obspy import Stream as ObspyStream

# pylint: disable=W0223
class Stream(ObspyStream):
    '''
    Overwrite Stream write routine to add additional formats
    '''
    # pylint: disable=W0221
    def write(self, filename, **kwargs):
        '''
        Write information from geomag database to geomag specified format

        See write routine of formats for list of keywords.
        All write routines take this object as first argument and then
        the filename and keyword.
        '''
        import imp
        format_spec = kwargs.get('format', 'iaga2002').lower()
        try:
            fptr, pathname, description = imp.find_module('chis/geomag/data/formats/'+format_spec)
            write_format = imp.load_module('write', fptr, pathname, description)
            return write_format.write(self, filename, **kwargs)
        except ImportError:
            super(Stream, self).write(self, filename, **kwargs)
            