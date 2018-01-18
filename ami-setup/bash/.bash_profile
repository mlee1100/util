# .bash_profile
FILEDIR=$(dirname ${BASH_SOURCE[0]})

# Get Usernames and password variables (this script should not be in the repo)
if [ -f ~/.bash_security ]; then
    . ~/.bash_security
fi

if [ -f $FILEDIR/.bash_security ]; then
  . $FILEDIR/.bash_security
fi

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

if [ -f $FILEDIR/.bashrc ]; then
  . $FILEDIR/.bashrc
fi

# User specific environment and startup programs
PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH