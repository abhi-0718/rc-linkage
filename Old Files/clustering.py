
import numpy as np
import sentence_transformers as SentenceTransformer
from numpy.linalg import norm

class Clustering():

    """
    This is the class that stores all the functions from digit comparision, cosine_similarity module 
    and the actual cluster formation algorithm for assigning the same classes for similar addresses
    """

    def cosine_function( embeddings1, embeddings2):

        """
            This function implements the Cosine Similarity module
            Args:
                embeddings1: These are the embeddings of the first address
                embeddings2: These are the embeddings of the second address
            
            Return:
                cosine: This is the similarity percentage between the two addresses to be compared
        """
        try:
            A = embeddings1
            B = embeddings2
            cosine = np.dot(A, B)/(norm(A)*norm(B))
            return cosine

        except Exception as err:
            print(err)

    def model_declare():

        """
            This function is utilized to load the stored model
            
            Return:
                model: This variable returns the stored model
        """
        try:

            model_path = 'local/path/to/model'
            model = SentenceTransformer(model_path)

            return model

        except Exception as err:
            print(err)

    def convert_to_single( address, city, state, zip):

        """
            This function converts all the columns into a single instance
            Args:
                address: This has the initial street address
                city: This is the city column 
                state: This is the state column
                zip: This variable represents the zip code of the regions
            
            Return:
                result: This returns the combination of all the columns together
        """
        try:
            
            result = []
            for some in range(0, len(address)):
                res = []
                res.append(str(address[some]))
                res.append(str(city[some]))
                res.append(str(state[some]))
                res.append(str(zip[some]))
                ress = ",".join(res)
                result.append(ress)

            return result

        except Exception as err:
            print(err)


    def Digit_Removal( main_address, loop_address):

        """
            This function removes the digits from the main and secondary addresses, thus prepares it
            for the cosine functionality
            
            Args:
                main_address: This is the main_address in the comparision
                loop_address: This is the looping address in the comparision(second)
            
            Returns:
                main_string: Combination of just the words for the main_address
                second_string: Combination of just the words for the second_address
        """
        try:

            words = [word for word in main_address if not word.isdigit()]
            main_string = ' '.join(words)

            words = [word for word in loop_address if not word.isdigit()]
            second_string = ' '.join(words)

            return main_string, second_string
        
        except Exception as err:
            print(err)


    def Digit_Comparision( main_address, loop_address):

        """
            This function compares the digits between the main_address, loop_address
            Args:
                main_address: This is the main_address in the comparision
                loop_address: This is the looping address in the comparision(second)
            
            Returns:
                1 : True (Same)
                0 : False (Not same)
        """
        try:
            import collections
            main_number = [word for word in main_address if word.isdigit()]
            second_number = [word for word in loop_address if word.isdigit()]
            if collections.Counter(main_number) == collections.Counter(second_number):
                return 1
            else:
                return 0

        except Exception as err:
            print(err)

    def Cluster_Formation(  main_address, loop_address, Cluster, length, Cluster_Assign, main_assign, second_assign):
        
        """
            This is the cluster formation algorithm
            Args:

                main_address: This is the main_address in the comparision
                loop_address: This is the looping address in the comparision(second)
                Cluster: This is a dictionary storing all the clusters
                Cluster_Assign: This variable has all the information about the clusters
                main_assign: This is the index of the main_address
                second_assign: This is the index of the secondary_address
            
            Return:
                Cluster : This is a dictionary storing all the clusters
                Cluster_Assign : This variable has all the information about the clusters
        """
        try:
            main_present = 0
            second_present = 0
            result = []
            main_address = ' '.join(main_address)
            loop_address = ' '.join(loop_address)

            for clusters in Cluster.values():
                if main_address in clusters:

                    key = list(Cluster.values()).index(clusters)
                    main_index = list(Cluster.keys())[key]
                    main_present = 1
                    break

                else:
                    main_present = 0

            for clusters in Cluster.values():
                if loop_address in clusters:

                    key = list(Cluster.values()).index(clusters)
                    second_index = list(Cluster.keys())[key]
                    second_present = 1
                    break
                else:
                    second_present = 0

            if main_present == 1 and second_present == 1:

                main_result = Cluster[main_index]
                second_result = Cluster[second_index]

                if main_index > second_index:
                    main_result.extend(second_result)
                    Cluster[main_index] = main_result
                    Cluster_Assign[main_assign] = main_index
                    Cluster_Assign[second_assign] = main_index
                    del Cluster[second_index]

                elif second_index > main_index:
                    second_result.extend(main_result)
                    Cluster[second_index] = second_result
                    Cluster_Assign[main_assign] = second_index
                    Cluster_Assign[second_assign] = second_index
                    del Cluster[main_index]

                length = len(Cluster)+1

            elif main_present == 1 and second_present == 0:

                main_result = Cluster[main_index]
                main_result.append(loop_address)
                Cluster[main_index] = main_result
                Cluster_Assign[main_assign] = main_index
                Cluster_Assign[second_assign] = main_index
                length = len(Cluster)+1

            elif main_present == 0 and second_present == 1:

                second_result = Cluster[second_index]
                second_result.append(main_address)
                Cluster[second_index] = second_result
                Cluster_Assign[main_assign] = second_index
                Cluster_Assign[second_assign] = second_index
                length = len(Cluster)+1

            else:

                result.append(main_address)
                result.append(loop_address)
                Cluster[length] = result
                Cluster_Assign[main_assign] = length
                Cluster_Assign[second_assign] = length
                length = length + 1

            return Cluster, length, Cluster_Assign

        except Exception as err:
            print(err)
