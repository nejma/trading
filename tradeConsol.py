import io


# Read and process one chunck of trades into memory
def processData(trades, moreChunksComing):

	#s = io.StringIO(trades)
	
	# Using the Loop-and-a-half construct to account for the chunks 'till the last one included.
	for trade in trades:
		timestamp, symbol, quantity, price  = trade.split(',')
		#if symbol != 'bec': continue
		timestamp 	= int(timestamp)
		quantity 	= int(quantity)
		price		= int(price)
		priceToQuantityDict={}
		maxPrice	= price
		maxTimeGap=0 
	
		if symbol not in singleChunckConsolidatedTradesDict.keys():
			priceToQuantityDict={price:quantity}
		else:
			prevPrice = singleChunckConsolidatedTradesDict[symbol][0]
			maxPrice = price if prevPrice < price else prevPrice
			
			previousTradeTiming =  singleChunckConsolidatedTradesDict[symbol][1] 
			#print("Symbol: {}, previous timestamp: {}, current timestamp: {} ".format(symbol, previousTradeTiming[1], timestamp)) 

			currentTimeGap = timestamp - previousTradeTiming[1]
			maxTimeGap = currentTimeGap if currentTimeGap > previousTradeTiming[0] else previousTradeTiming[0]
			
			priceToQuantityDict = singleChunckConsolidatedTradesDict[symbol][2]
			if price in priceToQuantityDict.keys():
				currentQuantityForThisPrice = priceToQuantityDict[price]
				newQuantityForThisPrice = quantity + currentQuantityForThisPrice
				priceToQuantityDict[price] = newQuantityForThisPrice
			else:
				priceToQuantityDict[price] = quantity
			
		singleChunckConsolidatedTradesDict[symbol] = [maxPrice, [maxTimeGap, timestamp], priceToQuantityDict ]
		#print(singleChunckConsolidatedTradesDict[symbol])
		if moreChunksComing == False: 
			return

def consolidatedTradeOutput(OUTPUT_PATH):
	
	# Writing to file 
	with open(OUTPUT_PATH, "w") as op: 
		
		for symbol in sorted(singleChunckConsolidatedTradesDict.keys()):
		
			# total volume for this symbol
			priceToQuantityDict = singleChunckConsolidatedTradesDict[symbol][2]
			totalVolume = sum(priceToQuantityDict.values())

			# Weighted average price for this symbol
			partialWeightedAveragePrice = 0	
			for priceKey in priceToQuantityDict.keys():
				partialWeightedAveragePrice = partialWeightedAveragePrice + (priceKey * priceToQuantityDict[priceKey])
				
			calculatedWeightedAveragePrice = partialWeightedAveragePrice / totalVolume
			weightedAvgPrice = "{0:.2f}".format(calculatedWeightedAveragePrice)
			maxPrice = singleChunckConsolidatedTradesDict[symbol][0]
			maxTimeGap = singleChunckConsolidatedTradesDict[symbol][1][0]
			# <symbol>,<MaxTimeGap>,<Volume>,<WeightedAveragePrice>,<MaxPrice>
			consolidatedTrade=f'{symbol},{maxTimeGap},{totalVolume},{weightedAvgPrice},{maxPrice}'
			
			# Print to stdout so that output can be piped and to OUTPUT_PATH file 
			print(consolidatedTrade)
			op.writelines(consolidatedTrade + "\n") 	
		
# Read external file (not a stream)
def readInputFile(INPUT_PATH):

	#print("\nUsing streams") 
	chunkSize = 300     
	chunk = []
	
	chunkSlot=1
	with open (INPUT_PATH, "r") as trades:
		for trade in trades:
			chunk.append(trade.replace("\n",""))
			if len(chunk) == chunkSize:
				print("Processing chunk # ", chunkSlot)
				
				# this allows for concurrent processing
				processData(chunk,True)
				
				chunk = []
				chunkSlot+=1
			
		#Process last chunk		
		if len(chunk) > 0:
			processData(chunk, False)
		
#---------------------------- main --------------------------
	

	# Python program to compile trade
	
if __name__ == "__main__":

	INPUT_PATH	=	"input.csv"
	OUTPUT_PATH	=	"output.csv"
	
	#singleChunckConsolidatedTradesDict[symbol] = [maxPrice, [maxTimeGap, lastTimestamp], {price1: qt1, price2:qt2, ...} ]
	singleChunckConsolidatedTradesDict = {}
	
	readInputFile(INPUT_PATH)
	
	consolidatedTradeOutput(OUTPUT_PATH)
