# .bash_profile
FILEDIR=$(dirname ${BASH_SOURCE[0]})

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