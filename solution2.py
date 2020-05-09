from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_tuple_totals,
                   reducer=self.reducer_output_results)
                ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings (self, key, values):
        yield key, sum(values)

    def mapper_tuple_totals(self, movieID, ratingTotal):
        yield None, (ratingTotal, movieID)

    def reducer_output_results(self, n, ratingTotalMovieID):
        for c in sorted(ratingTotalMovieID, reverse=False):
            yield c[1], c[0]

if __name__ == '__main__':
    RatingsBreakdown.run()
