# rusty-horse : An xkcd-style passphrase generator

This is yet another variation of the memorable random word based passphrase
generation algorithm that was popularized by xkcd:

[ ![](https://imgs.xkcd.com/comics/password_strength.png "To anyone who
understands information theory and security and is in an infuriating argument
with someone who does not (possibly involving mixed case), I sincerely
apologize.") ](https://xkcd.com/936/)

Why would I implement yet another passphrase generator when there are many good
ones out there?

- I am not fully satisfied with the common English word lists that I found
  online, and wanted to see if I could do it better with some help from the more
  extensive and multilingual [Google Books Ngrams
  dataset](http://storage.googleapis.com/books/ngrams/books/datasetsv3.html)
- It's a good excuse to have fun with big-ish data natural language processing
