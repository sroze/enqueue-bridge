<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Sam\Symfony\Bridge\EnqueueMessage;

use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpQueue;
use Interop\Queue\PsrContext;
use Symfony\Component\Message\Transport\ReceiverInterface;
use Symfony\Component\Message\Transport\Serialization\DecoderInterface;

/**
 * Symfony Message receivers to get messages from php-enqueue consumers.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class QueueInteropReceiver implements ReceiverInterface
{
    /**
     * @var DecoderInterface
     */
    private $messageDecoder;

    /**
     * @var PsrContext
     */
    private $context;

    /**
     * @var float
     */
    private $receiveTimeout;


    /**
     * @var string
     */
    private $queueName;

    public function __construct(DecoderInterface $messageDecoder, PsrContext $context, string $queueName)
    {
        $this->messageDecoder = $messageDecoder;
        $this->context = $context;
        $this->queueName = $queueName;

        $this->receiveTimeout = 1000; // 1s
    }

    /**
     * {@inheritdoc}
     */
    public function receive(): iterable
    {
        $destination = $this->context->createQueue($this->queueName);
        $consumer = $this->context->createConsumer($destination);

        if ($this->context instanceof AmqpContext) {
            $destination = $this->context->createQueue($this->queueName);
            $destination->addFlag(AmqpQueue::FLAG_DURABLE);

            $this->context->declareQueue($destination);
        }

        while (true) {
            if (null === ($message = $consumer->receive($this->receiveTimeout))) {
                continue;
            }

            try {
                yield $this->messageDecoder->decode([
                    'body' => $message->getBody(),
                    'headers' => $message->getHeaders(),
                    'properties' => $message->getProperties(),
                ]);

                $consumer->acknowledge($message);
            } catch (RejectMessageException $e) {
                $consumer->reject($message);
            } catch (RequeueMessageException $e) {
                $consumer->reject($message, true);
            }
        }
    }
}
