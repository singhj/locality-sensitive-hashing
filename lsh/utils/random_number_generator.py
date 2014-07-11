import random
import threading

def generate_random_numbers(n=1, bits=32):
    """
        Generates n random numbers.
        :param n: number of random numbers to generate
        :param bits: number of bits each random number should contain.
        :return: list of long integer random numbers.
    """
    return [random.getrandbits(bits) for _ in range(0, n)]

class RNG(object):
    """
        Implements a thread safe, random number generator. It generates a list of n random numbers. By default
        all numbers are 32bit randomly generated numbers. Users can specify the number of random numbers to be
        generated.

        This class is designed such that all threads will have access the same random numbers. This implementation
        is specific to the needs of our minhash algorithm. In order to implement n number hash functions we use n
        number random generated numbers and XOR those numbers against hashed values. All documents we process need to
        use the same random numbers. In order for our LSH implementation to be scalable each thread / instance should
        have it's own set of generated numbers and this class enables us to do this.

    """

    __lock = threading.Lock()
    __instance = None
    __random_numbers = None

    @classmethod
    def instance(cls, n=1, bits=32):
        if not cls.__instance and not cls.__random_numbers:
            with cls.__lock:
                if not cls.__instance and not cls.__random_numbers:
                    cls.__instance = RNG()
                    cls.__random_numbers = generate_random_numbers(n, bits)
        return cls.__random_numbers

