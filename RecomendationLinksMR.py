from mrjob.job import MRJob
from mrjob.job import MRStep

import sys
class RecomendationLinksMR(MRJob):

    def steps(self):
        return [

            MRStep(mapper=self.direct_friends_mapper,
                   reducer=self.super_reducer),
        ]
        
    def direct_friends_mapper(self, key, line):

        # For the case we don´t have a comma after the fisrt position  (marvel_graph.txt)
        # e.g. 100 200 300 400 500 600
        if(',' not in line):
            values = list(line.split(" "))
            person = values[0]
            friends = values[1:len(values) - 1]
        else:
        # For the case we have a comma after the first hero (data.txt)
        # e.g. 100, 200 300 400 500 600
            values = line.split(',')
            person = values[0]
            friends = values[1].strip().split(' ')

        for friend in friends:
            direct_friend = [friend, -1]                  # Set direct friendship as -1
            yield person, direct_friend
        
        for i in range(0, len(friends)):                   # Cross data and get all possible friends
            for j in range(i + 1, len(friends)):

                possible_friend1 = [friends[j], person]
                yield friends[i], possible_friend1

                possible_friend2 = [friends[i], person]
                yield friends[j], possible_friend2



    def super_reducer(self, key, values):
        map = {}

        for friend in values:
            to_user = friend[0]                     # Get current hero
            mutual_friend = friend[1]               # Get mutual friend
            already_friend = (mutual_friend == -1)  # Verify if is already a direct friendship
            

            if to_user in map:                     # Separate those that are not a friend yet
                if(already_friend):
                    map[to_user] = None
                elif(map[to_user] != None):
                    map[to_user] = mutual_friend
            else:
                if(already_friend):
                    map[to_user] = None
                else:
                    map[to_user] = mutual_friend

        yield key, self.get_only_the_recomendations(map)


    def get_only_the_recomendations(self, map):     # Function to remove those with NULL value and return only the recomedation
        final_map = []
        for a in map:
            if(map[a] != None):
                final_map.append(a)
        return final_map


if __name__ == '__main__':
    RecomendationLinksMR.run()
