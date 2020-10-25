# Creating-N-gram-profile-for-a-Wikipedia-Corpus

Objectives

● Basic features of Hadoop distributed file system and MapReduce
● Creating NGram profiles using Hadoop MapReduce

1. Introduction
An N-gram is a contiguous sequence of N items from a given sequence of text or speech. An N-gram model is a type of probabilistic language model for predicting 
the next item in such a sequence in the form of a (n-1). N-gram models are widely used in statistical natural language processing. In speech recognition, phonemes
and sequence of phonemes are modeled using a N-gram distribution. For sequences of words, the 1-grams (aka unigram) generated from “We analyze large dataset” are 
(“We”, “analyze”, “large”, “dataset”). For the same sentence, the 2-grams (aka bigram) are (“__ , We”, “We, analyze”, “analyze, large”, “large, dataset”, “dataset, __”). 
Here, “__” represents the empty space before and after the sentence. N-grams are used for various applications such as approximate matching, plagiarism detection, 
searching for the similar documents, automatic authorship detection, and linguistic cultural trend analysis. Google’s Ngram Viewer is a good example of N-gram analysis1. 
(https://books.google.com/ngrams/info)
I will create N-gram profile of the corpus of selected Wikipedia articles2.
I will: (1) extract all the unigrams,
(2) compute the frequency of each unigram per page and also over the corpus, and 
(3) rank the unigram based on these frequencies. As a corpus for this project, I will be provided around 1GB of dataset selected from Wikipedia articles.
My computing environment will be MapReduce.

