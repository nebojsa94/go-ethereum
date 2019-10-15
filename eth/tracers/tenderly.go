package tracers

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
)

type Tenderly struct {
	descended bool

	status bool

	addresses [][]byte
}

func NewTenderlyTracer() *Tenderly {
	return &Tenderly{}
}

func (tracer *Tenderly) CaptureStart(from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	tracer.status = true
	tracer.addresses = append(tracer.addresses, from.Bytes())
	tracer.addresses = append(tracer.addresses, to.Bytes())
	return nil
}

func (tracer *Tenderly) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if tracer.descended {
		tracer.addresses = add(tracer.addresses, contract.CodeAddr.Bytes())
		tracer.descended = false
	}

	if op == vm.CREATE || op == vm.CREATE2 {
		tracer.descended = true
	}

	if op == vm.CALL || op == vm.CALLCODE || op == vm.DELEGATECALL || op == vm.STATICCALL {
		if _, ok := vm.PrecompiledContractsByzantium[*contract.CodeAddr]; ok {
			return nil
		}

		tracer.descended = true
	}

	return nil
}

func (tracer *Tenderly) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if depth == 1 {
		tracer.status = false
	}

	return nil
}

func (tracer *Tenderly) CaptureEnd(output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}

func (tracer *Tenderly) GetResult() (json.RawMessage, error) {
	return json.Marshal(tracer.addresses)
}

func add(slice [][]byte, item []byte) [][]byte {
	for _, i := range slice {
		if common.BytesToHash(i) == common.BytesToHash(item) {
			return slice
		}
	}

	return append(slice, item)
}
