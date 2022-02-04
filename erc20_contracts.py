from sys import get_asyncgen_hooks
from attributedict.collections import AttributeDict
from web3 import Web3
import ray
ray.init()

web3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/b7771067f61c4314baa6d566cbfd88b7'))

balanceOfByte = "70a08231"
totalSupplyByte = "18160ddd"
transferByte = "a9059cbb"
transferFromByte = "23b872dd"
approveByte = "095ea7b3"
allowanceByte = "dd62ed3e"

@ray.remote
def getData(start,end):
    ret = []
    for i in range(start,end):
        print(start,"reached here:",end - i)
        txnsA = web3.eth.get_block(i)
        txns = AttributeDict(txnsA)
        for txnHash in txns['transactions']:
            rec = AttributeDict(web3.eth.get_transaction_receipt(txnHash)).contractAddress
            if rec != None:
                ercCode = web3.eth.get_code(rec).hex()
                # print(ercCode)
                if balanceOfByte in ercCode and totalSupplyByte in ercCode and transferByte in ercCode and transferFromByte in ercCode and approveByte in ercCode and allowanceByte in ercCode:
                    print(txnHash.hex())
                    ret.append(txnHash.hex())
    return ret

finalResult = []

finalResult.append(getData.remote(14130000,14130125))
finalResult.append(getData.remote(14130125,14130250))
finalResult.append(getData.remote(14130250,14130375))
finalResult.append(getData.remote(14130375,14130500))
finalResult.append(getData.remote(14130500,14130625))
finalResult.append(getData.remote(14130625,14130750))

res = ray.get(finalResult)

print(res)