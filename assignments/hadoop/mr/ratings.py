from mrjob.job import MRJob
class Rating_count(MRJob):
        def mapper(self, _, line):
                (userID,movieID, rating, timestamp) = line.split(',')
                yield(rating, 1)
        def reducer(self, rate, counts):
                if rate != "rating":
                        yield(rate, sum(counts))

if __name__ == '_main_':
	Rating_count.run()