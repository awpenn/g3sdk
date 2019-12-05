inco = ['other', 'african american or black', 'na', 'american indian/alaska native', 'AD', 'Female', 'not available/not applicable', '23']

def prettifier(rawInput):
    connectives = ['and', 'or']

    tada = []
    def checkForSlash(input):
        if "/" in input:
            i = input.find("/")+1
            t = input[:i].lower() + input[i:].capitalize()
            build(t)

        else:
            build(input)

    def build(input):
        for word in input.split():
            if word not in connectives and word is not 'na':
                first = word[0].capitalize()
                rest = word[1:]
                tada.append(first+rest)

            else:
                if word is not 'na':
                    tada.append(word)
                else:
                    tada.append('NA')

    checkForSlash(pheno)
    return " ".join(tada)
