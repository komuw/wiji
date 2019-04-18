import typing


class AVys:

    x: typing.Union[None, typing.Type[AVys]] = None

    def cool(self) -> int:
        reveal_type(self)
        return 34


class Bbb(AVys):
    x = AVys


print("type of AVys")
reveal_type(AVys)


a = AVys()
print("type of a")
reveal_type(a)
