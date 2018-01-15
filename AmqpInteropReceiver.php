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

use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpQueue;
use Symfony\Component\Message\Transport\ReceiverInterface;
use Symfony\Component\Message\Transport\Serialization\DecoderInterface;

/**
 * Symfony Message receivers to get messages from php-enqueue consumers.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class AmqpInteropReceiver implements ReceiverInterface
{
    /**
     * @var DecoderInterface
     */
    private $messageDecoder;

    /**
     * @var AmqpContext
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

    public function __construct(DecoderInterface $messageDecoder, AmqpContext $context, string $queueName)
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
        $destination->addFlag(AmqpQueue::FLAG_DURABLE);
        $this->context->declareQueue($destination);

        $consumer = $this->context->createConsumer($destination);

        /** @var AmqpConsumer|null $currentConsumer */
        $currentConsumer = null;

        /** @var AmqpMessage|null $currentMessage */
        $currentMessage = null;

        $this->context->subscribe($consumer, function(AmqpMessage $message, AmqpConsumer $consumer) use (&$currentMessage, &$currentConsumer) {
            $currentMessage = $message;
            $currentConsumer = $consumer;

            return false;
        });

        while (true) {
            $currentConsumer = null;
            $currentMessage = null;

            $this->context->consume($this->receiveTimeout);

            if ($currentMessage) {
                try {
                    yield $this->messageDecoder->decode([
                        'body' => $currentMessage->getBody(),
                        'headers' => $currentMessage->getHeaders(),
                        'properties' => $currentMessage->getProperties(),
                    ]);

                    $currentConsumer->acknowledge($currentMessage);
                } catch (RejectMessageException $e) {
                    $currentConsumer->reject($currentMessage);
                } catch (RequeueMessageException $e) {
                    $currentConsumer->reject($currentMessage, true);
                }
            }
        }
    }
}
