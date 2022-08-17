# NOTE: before running this script be sure to set the 
# working directory to the location of the Dockerfile

bashstring <- "sudo docker build -t eminty/prone_nlp:0.2 ."
system(bashstring)

# bash_string = """
# sudo docker push eminty/prone_nlp:0.2
# """
# os.system(bash_string)