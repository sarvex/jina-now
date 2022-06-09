from re import sub


class BetterEnum:
    def __iter__(self):
        return [getattr(self, x) for x in dir(self) if ('__' not in x)].__iter__()


def camel_case(s):
    return sub(r"(_|-)+", " ", s).title().replace(" ", "")


def to_camel_case(text):
    s = text.replace("-", " ").replace("_", " ")
    s = s.split()
    print(s)
    if len(text) == 0:
        return text
    return ''.join(i.capitalize() for i in s)


class Apps(BetterEnum):
    TEXT_TO_IMAGE = 'text_to_image'
    IMAGE_TO_TEXT = 'image_to_text'
    IMAGE_TO_IMAGE = 'image_to_image'
    MUSIC_TO_MUSIC = 'music_to_music'


for app in Apps():
    print(app)
    # print(to_camel_case(app))

# print(50*'=')
#
# for app in Apps():
#     print(camel_case(app.value))
#
