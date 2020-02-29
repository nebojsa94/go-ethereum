package tracers

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/vm"
)

type Call struct {
	CallType      string          `json:"callType,omitempty"`
	From          common.Address  `json:"from,omitempty"`
	To            *common.Address `json:"to,omitempty"`
	Gas           hexutil.Uint    `json:"gas,omitempty"`
	GasUsed       *hexutil.Uint64 `json:"gasUsed,omitempty"`
	Value         *hexutil.Big    `json:"value,omitempty"`
	Address       *common.Address `json:"address,omitempty"`
	Balance       *hexutil.Big    `json:"balance,omitempty"`
	RefundAddress *common.Address `json:"refundAddress,omitempty"`
	Error         *string         `json:"error,omitempty"`
	Subtraces     int             `json:"subtraces,omitempty"`
	TraceAddress  []int           `json:"traceAddress,omitempty"`
	Type          string          `json:"type,omitempty"`
	Input         hexutil.Bytes   `json:"input,omitempty"`
	Output        hexutil.Bytes   `json:"output,omitempty"`
	gasIn         uint64
	gasCost       uint64
	outOff        uint64
	outLen        uint64
}

type CallTrace []*Call

func (s *CallTrace) len() int {
	return len(*s)
}

func (s CallTrace) peek(k int) *Call {
	return s[k]
}

func (s *CallTrace) push(c *Call) {
	*s = append(*s, c)
}

func (s *CallTrace) pop() *Call {
	l := len(*s)
	a := *s
	c := a[l-1]
	*s = a[:l-1]
	return c
}

func (s *CallTrace) first() *Call {
	return s.peek(0)
}

func (s *CallTrace) last() *Call {
	return s.peek(s.len() - 1)
}

type Tenderly struct {
	descended bool

	status bool

	callTrace CallTrace
	tracePos  []int
}

func NewTenderlyTracer() *Tenderly {
	return &Tenderly{}
}

func (tracer *Tenderly) CaptureStart(from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) error {
	var (
		op        vm.OpCode
		toAddress *common.Address
		address   *common.Address
	)

	if create {
		op = vm.CREATE
		address = &to
	} else {
		op = vm.CALL
		toAddress = &to
	}

	tracer.callTrace.push(&Call{
		CallType:     op.String(),
		From:         from,
		To:           toAddress,
		Gas:          hexutil.Uint(gas),
		Value:        (*hexutil.Big)(value),
		Address:      address,
		TraceAddress: []int{},
		Input:        input,
	})

	tracer.tracePos = append(tracer.tracePos, 0)
	tracer.status = true

	return nil
}

func (tracer *Tenderly) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if err != nil {
		return tracer.CaptureFault(env, pc, op, gas, cost, memory, stack, contract, depth, err)
	}

	if op == vm.CREATE || op == vm.CREATE2 {
		inOff := peek(stack, 1)
		inEnd := new(big.Int)
		inEnd = inEnd.Add(inOff, peek(stack, 2))

		tracer.callTrace.push(&Call{
			CallType:     op.String(),
			From:         contract.Address(),
			Input:        slice(memory, inOff.Int64(), inEnd.Int64()),
			Value:        (*hexutil.Big)(peek(stack, 0)),
			Type:         vm.CREATE.String(),
			TraceAddress: tracer.pushAddress(),
			gasIn:        gas,
			gasCost:      cost,
		})

		tracer.descended = true
		return nil
	}

	if op == vm.SELFDESTRUCT {
		tracer.callTrace.push(&Call{
			Address:       contract.CodeAddr,
			Balance:       (*hexutil.Big)(env.StateDB.GetBalance(contract.Address())),
			RefundAddress: &tracer.callTrace[len(tracer.callTrace)-1].From,
			Subtraces:     0,
			TraceAddress:  tracer.pushAddress(),
			Type:          op.String(),
		})

		tracer.popAddress()
		return nil
	}

	if op == vm.CALL || op == vm.CALLCODE || op == vm.STATICCALL || op == vm.DELEGATECALL {
		to := common.BigToAddress(peek(stack, 1))
		if _, ok := vm.PrecompiledContractsIstanbul[to]; ok {
			return nil
		}

		off := 0
		if op == vm.CALL || op == vm.CALLCODE {
			off = 1
		}

		inOff := peek(stack, 2+off)
		inEnd := new(big.Int)
		inEnd = inEnd.Add(inOff, peek(stack, 3+off))

		call := Call{
			CallType:     op.String(),
			From:         contract.Address(),
			To:           &to,
			Input:        slice(memory, inOff.Int64(), inEnd.Int64()),
			outOff:       peek(stack, 4+off).Uint64(),
			outLen:       peek(stack, 5+off).Uint64(),
			Type:         vm.CALL.String(),
			TraceAddress: tracer.pushAddress(),
			gasIn:        gas,
			gasCost:      cost,
		}

		if op == vm.CALL || op == vm.CALLCODE {
			call.Value = (*hexutil.Big)(peek(stack, 2))
		}

		tracer.callTrace.push(&call)
		tracer.descended = true
		return nil
	}

	if tracer.descended {
		if depth >= len(tracer.tracePos) {
			tracer.callTrace.last().Gas = hexutil.Uint(gas)
		} else {
			tracer.callTrace.last().Gas = hexutil.Uint(gas + tracer.callTrace.last().gasCost - tracer.callTrace.last().gasIn)
		}

		tracer.descended = false
	}

	if op == vm.REVERT {
		tracer.callTrace[len(tracer.callTrace)-1].Error = newErr("execution error")
		return nil
	}

	if depth == len(tracer.tracePos)-1 {
		call := tracer.callTrace.peek(tracer.tracePos[len(tracer.tracePos)-1])

		if call.CallType == vm.CREATE.String() || call.CallType == vm.CREATE2.String() {
			gasUsed := call.gasIn - call.gasCost - gas
			call.GasUsed = (*hexutil.Uint64)(&gasUsed)

			var ret *big.Int
			ret = peek(stack, 0)
			if ret != big.NewInt(0) {
				address := common.BigToAddress(ret)
				call.Address = &address
				call.Output = env.StateDB.GetCode(common.BigToAddress(ret))
			} else if call.Error == nil {
				call.Error = newErr("internal failure")
			}
		} else {
			if call.Gas != 0 {
				gasUsed := call.gasIn - call.gasCost + uint64(call.Gas) - gas
				call.GasUsed = (*hexutil.Uint64)(&gasUsed)
				ret := peek(stack, 1)
				if ret != big.NewInt(0) {
					call.Output = slice(memory, int64(call.outOff), int64(call.outOff+call.outLen))
				} else if call.Error == nil {
					call.Error = newErr("internal failure")
				}
			}
		}

		tracer.popAddress()
	}

	return nil
}

func (tracer *Tenderly) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if depth == 1 {
		tracer.status = false
	}

	if tracer.callTrace.last().Error != nil {
		return nil
	}

	call := tracer.callTrace.pop()
	call.Error = newErr(err.Error())
	gasUsed := uint64(call.Gas)
	call.GasUsed = (*hexutil.Uint64)(&gasUsed)

	if depth == 1 {
		tracer.callTrace.push(call)
		return nil
	}

	tracer.popAddress()
	return nil
}

func (tracer *Tenderly) CaptureEnd(output []byte, gasUsed uint64, t time.Duration, err error) error {
	tracer.callTrace.first().GasUsed = (*hexutil.Uint64)(&gasUsed)
	if err != nil {
		tracer.callTrace.first().Error = newErr(err.Error())
		return nil
	}

	tracer.callTrace.first().Output = output
	return nil
}

func (tracer *Tenderly) pushAddress() (cpy []int) {
	parent := tracer.callTrace[tracer.tracePos[len(tracer.tracePos)-1]]
	tracer.tracePos = append(tracer.tracePos, tracer.callTrace.len())
	parent.Subtraces += 1

	cpy = make([]int, len(parent.TraceAddress)+1)
	copy(cpy, append(parent.TraceAddress, parent.Subtraces-1))

	return
}

func (tracer *Tenderly) popAddress() {
	tracer.tracePos = tracer.tracePos[:len(tracer.tracePos)-1]
}

func peek(stack *vm.Stack, id int) *big.Int {
	if len(stack.Data()) <= id {
		return new(big.Int)
	}

	val := stack.Data()[len(stack.Data())-id-1].Bytes()
	ret := new(big.Int).SetBytes(val)

	return ret
}

func slice(memory *vm.Memory, begin, end int64) []byte {
	if begin < 0 || begin > end || end > int64(memory.Len()) {
		return []byte{}
	}
	return memory.GetCopy(begin, end-begin)
}

func add(slice [][]byte, item []byte) [][]byte {
	for _, i := range slice {
		if common.BytesToHash(i) == common.BytesToHash(item) {
			return slice
		}
	}

	return append(slice, item)
}

func (tracer *Tenderly) GetResult() (json.RawMessage, error) {
	return json.Marshal(tracer.callTrace)
}

func newErr(text string) *string {
	return &text
}
