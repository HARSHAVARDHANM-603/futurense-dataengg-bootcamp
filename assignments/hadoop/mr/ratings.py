from mrjob.job import MRJob
class Rating_count(MRJob):
        def mapper(self, _, line):
                (userID,movieID, rating, timestamp) = line.split(',')
                if type(rating) is float:
                        yield(rating, 1)
        def reducer(self, rate, counts):
                yield(rate, sum(counts))

if __name__ == '_main_':
	Rating_count.run()