from sys import get_asyncgen_hooks
from attributedict.collections import AttributeDict
from web3 import Web3
import ray
ray.init()

from clickhouse_driver import Client

client = Client(host='localhost')

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
    for atBlock in range(start,end):
        print(start,"reached here:",end - atBlock)
        txnsA = web3.eth.get_block(atBlock)
        txns = AttributeDict(txnsA)
        for txnHash in txns['transactions']:
            contractAdd = AttributeDict(web3.eth.get_transaction_receipt(txnHash)).contractAddress
            if contractAdd != None:
                ercCode = web3.eth.get_code(contractAdd).hex()
                creator = AttributeDict(web3.eth.get_transaction_receipt(txnHash))['from']
                if balanceOfByte in ercCode and totalSupplyByte in ercCode and transferByte in ercCode and transferFromByte in ercCode and approveByte in ercCode and allowanceByte in ercCode:
                    client.execute('INSERT INTO ERC20_DATA.erc20_contracts (deployedAtBlock,contractAddress,creatorAddress,transactionHash) VALUES',[(atBlock,contractAdd,creator,txnHash.hex())])
                    
    return ret

finalResult = []

st = 1397553


for i in range(8):
    finalResult.append(getData.remote(st + (i*125),st + ((i+1)*125)))


res = ray.get(finalResult)

print(res)