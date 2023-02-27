from mrjob.job import MRJob

class Count(MRJob):
        def mapper(self, _, line):
              for word in line.split():
                    yield(word, 1)
        def reducer(self, word, counts):
              ans = dict()
              ans[word] = sum(counts)
              yield ans

if __name__ == '__main__':
        Count.run()

