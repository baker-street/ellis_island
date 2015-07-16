

from ellis_island.utils.misc import get_default_data_key


def test_get_default_data_key():
    key = get_default_data_key()
    assert key == 'LVlwUZaAf66MM_OYbuaxt--ssO0XrgE-0K0GFdjszmc='
