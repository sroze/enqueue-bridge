<?php
namespace Sam\Symfony\Bridge\EnqueueMessage;

use Interop\Queue\PsrContext;
use Interop\Queue\PsrMessage;
use Interop\Queue\PsrProcessor;
use Symfony\Component\Message\Asynchronous\Transport\ReceivedMessage;
use Symfony\Component\Message\MessageBusInterface;
use Symfony\Component\Serializer\Encoder\DecoderInterface;

class MessageBusProcessor implements PsrProcessor
{
    /**
     * @var MessageBusInterface
     */
    private $bus;

    /**
     * @var DecoderInterface
     */
    private $messageDecoder;

    public function __construct(MessageBusInterface $bus, DecoderInterface $messageDecoder)
    {
        $this->bus = $bus;
        $this->messageDecoder = $messageDecoder;
    }

    public function process(PsrMessage $message, PsrContext $context)
    {
        $busMessage = $this->messageDecoder->decode([
            'body' => $message->getBody(),
            'headers' => $message->getHeaders(),
            'properties' => $message->getProperties(),
        ]);

        if (!$busMessage instanceof ReceivedMessage) {
            $busMessage = new ReceivedMessage($message);
        }

        try {
            $this->bus->dispatch($busMessage);

            return self::ACK;
        } catch (RejectMessageException $e) {
            return self::REJECT;
        } catch (RequeueMessageException $e) {
            return self::REQUEUE;
        }
    }
}