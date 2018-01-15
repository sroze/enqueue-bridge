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

use Interop\Queue\PsrContext;
use Symfony\Component\Message\Transport\SenderInterface;
use Symfony\Component\Message\Transport\Serialization\EncoderInterface;

/**
 * Symfony Message sender to bridge Php-Enqueue producers.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class QueueInteropSender implements SenderInterface
{
    /**
     * @var EncoderInterface
     */
    private $messageEncoder;

    /**
     * @var PsrContext
     */
    private $context;

    /**
     * @var string
     */
    private $queueName;

    /**
     * @var string
     */
    private $topicName;

    /**
     * @var float
     */
    private $deliveryDelay;

    /**
     * @var float
     */
    private $timeToLive;

    /**
     * @var int
     */
    private $priority;

    public function __construct(EncoderInterface $messageEncoder, PsrContext $context, string $queueName = null, string $topicName = null)
    {
        $this->messageEncoder = $messageEncoder;
        $this->context = $context;

        $this->queueName = $queueName;
        $this->topicName = $topicName;

        if (false == ($this->queueName || $this->topicName)) {
            throw new \LogicException('Either queueName or topicName argument should be set.');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function send($message)
    {
        $encodedMessage = $this->messageEncoder->encode($message);


        if ($this->topicName) {
            $destination = $this->context->createTopic($this->topicName);
        } else {
            $destination = $this->context->createQueue($this->queueName);
        }


        $message = $this->context->createMessage(
            $encodedMessage['body'],
            $encodedMessage['properties'] ?? [],
            $encodedMessage['headers'] ?? []
        );

        $producer = $this->context->createProducer();

        if (null !== $this->deliveryDelay) {
            $producer->setDeliveryDelay($this->deliveryDelay);
        }
        if (null !== $this->priority) {
            $producer->setPriority($this->priority);
        }
        if (null !== $this->timeToLive) {
            $producer->setTimeToLive($this->timeToLive);
        }

        $producer->send($destination, $message);
    }

    /**
     * @return float
     */
    public function getDeliveryDelay(): float
    {
        return $this->deliveryDelay;
    }

    /**
     * @param float $deliveryDelay
     */
    public function setDeliveryDelay(float $deliveryDelay): void
    {
        $this->deliveryDelay = $deliveryDelay;
    }

    /**
     * @return float
     */
    public function getTimeToLive(): float
    {
        return $this->timeToLive;
    }

    /**
     * @param float $timeToLive
     */
    public function setTimeToLive(float $timeToLive): void
    {
        $this->timeToLive = $timeToLive;
    }

    /**
     * @return int
     */
    public function getPriority(): int
    {
        return $this->priority;
    }

    /**
     * @param int $priority
     */
    public function setPriority(int $priority): void
    {
        $this->priority = $priority;
    }
}
