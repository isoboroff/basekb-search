#!/bin/sh

mvn exec:java -Dexec.mainClass=gov.nist.basekb.SearchServer -Dexec.args="-c /Users/soboroff/basekb/basekb-search/config.dat -p 7777 -i /Users/soboroff/basekb/basekb-index -m /Users/soboroff/basekb/basekb-search/enttype.classifier"

