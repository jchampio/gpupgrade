package hub

import (
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/net/context"
)

func (h *Hub) CheckUpgrade(ctx context.Context, stream OutStreams) error {
	var wg sync.WaitGroup
	checkErrs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := h.UpgradeMaster(ctx, stream, true)
		if err != nil {
			checkErrs <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := h.ConvertPrimaries(ctx, true)
		if err != nil {
			checkErrs <- err
		}
	}()

	wg.Wait()
	close(checkErrs)

	var multiErr *multierror.Error
	for err := range checkErrs {
		multiErr = multierror.Append(multiErr, err)
	}

	return multiErr.ErrorOrNil()
}
