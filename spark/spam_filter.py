from reverend.thomas import Bayes
# from thomas.py import Bayes
import os

class NaiveBayesClassifier(object):
    def __init__(self, non_spam_train_dir, spam_train_dir):
        self.non_spam_train_dir = non_spam_train_dir
        self.spam_train_dir = spam_train_dir
        self.naive_bayes_classifier = Bayes()
        self.total_num_train_files = 0
        self.total_num_test_files = 0
        self.num_misclass = 0

    def make_single_line_from_body_of_file(self, filename):
        fd = open(filename)
        total = ''
        return total.join(line.strip() for line in fd)

    def train(self):
        for subdir, dirs, files in os.walk(self.non_spam_train_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                self.naive_bayes_classifier.train('nonspam', full_text_line)


        for subdir, dirs, files in os.walk(self.spam_train_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                self.naive_bayes_classifier.train('spam', full_text_line)

    def train_for_given_dirs(self, non_spam_train_dir, spam_train_dir):
        for subdir, dirs, files in os.walk(non_spam_train_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                self.naive_bayes_classifier.train('nonspam', full_text_line)


        for subdir, dirs, files in os.walk(spam_train_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                self.naive_bayes_classifier.train('spam', full_text_line)

    def train_two_files(self):
        fd = open(self.non_spam_train_dir, 'r')
        for line in fd:
            self.naive_bayes_classifier.train('nonspam', line)

        fd = open(self.spam_train_dir, 'r')
        for line in fd:
            self.naive_bayes_classifier.train('spam', line)

    def train_for_two_exogenous_files(self, non_spam_train_file, spam_train_file):
        if non_spam_train_file != '':
            fd = open(non_spam_train_file, 'r')
            for line in fd:
                self.naive_bayes_classifier.train('nonspam', line)
        if spam_train_file != '':
            fd = open(spam_train_file, 'r')
            for line in fd:
                self.naive_bayes_classifier.train('spam', line)

    def test(self, non_spam_test_dir, spam_test_dir):
        # rb.classify('sloths are so cute i love them') == 'good'
        for subdir, dirs, files in os.walk(non_spam_test_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                class_prob_vec = self.naive_bayes_classifier.guess(full_text_line)
                self.total_num_test_files += 1
                y_hat = class_prob_vec[0][0]
                if y_hat != 'nonspam':
                    self.num_misclass += 1

        for subdir, dirs, files in os.walk(spam_test_dir):
            for file_i in files:
                self.total_num_train_files += 1
                filename = os.path.join(subdir, file_i)
                full_text_line = self.make_single_line_from_body_of_file(filename)
                class_prob_vec = self.naive_bayes_classifier.guess(full_text_line)
                self.total_num_test_files += 1
                y_hat = class_prob_vec[0][0]
                print class_prob_vec
                if y_hat != 'spam':
                    self.num_misclass += 1
        misclass_rate = (self.num_misclass/float(self.total_num_test_files))
        accuracy = 1 - misclass_rate
        print 'Misclassification rate is %f' % misclass_rate
        print 'Accuracy is %f' % accuracy

    def make_single_line_from_body(self, text_body):
        total = ' '
        return total.join(line.strip() for line in text_body)

    def make_single_line_from_body2(self, text_body):
        total = ''
        for line in text_body:
            total += line + ' '
        return total

    def create_nonspam_spam_datasets(self, text_body):
        return text_body

    def classify(self, text_body):
        class_prob_vec = self.naive_bayes_classifier.guess(text_body)
        y_hat = 'nonspam'
        if len(class_prob_vec) != 0:
            y_hat = class_prob_vec[0][0]
        return y_hat


if __name__ == '__main__':
    import dill

    non_spam_train_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/text_cleaned_span_nonspam_emails/nonspam-train'
    non_spam_test_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/text_cleaned_span_nonspam_emails/nonspam-test'
    spam_train_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/text_cleaned_span_nonspam_emails/spam-train'
    spam_test_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/text_cleaned_span_nonspam_emails/spam-test'

    non_spam_single_train_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/nonspam_train_corporate.txt'
    non_spam_single_train_file2 = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/nonspam_train_corporate2.txt'
    spam_single_train_file = 'C:/Users/cschulze/PycharmProjects/Naive_Bayes_Spam_Classifier/spam_train_corporate.txt'

    nbc = NaiveBayesClassifier(non_spam_single_train_file, spam_single_train_file)
    nbc.train_two_files()
    nbc.train_for_given_dirs(non_spam_train_file, spam_train_file)
    nbc.train_for_two_exogenous_files(non_spam_single_train_file2, '')
    nbc.test(non_spam_test_file, spam_test_file)
    with open('naive_bayes_classifier.pkl', 'wb') as output:
        dill.dump(nbc, output)
