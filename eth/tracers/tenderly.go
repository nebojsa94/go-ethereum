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

	from      common.Address
	addresses []common.Address
}

func NewTenderlyTracer() *Tenderly {
	return &Tenderly{}
}

func (tracer *Tenderly) CaptureStart(from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	tracer.status = true
	tracer.from = from
	tracer.addresses = append(tracer.addresses, to)
	return nil
}

func (tracer *Tenderly) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if tracer.descended {
		tracer.addresses = append(tracer.addresses, *contract.CodeAddr)
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
	return json.Marshal(struct {
		From      common.Address   `json:"from"`
		Addresses []common.Address `json:"addresses"`
	}{
		From:      tracer.from,
		Addresses: tracer.addresses,
	})
}
